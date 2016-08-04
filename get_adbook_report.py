# The MIT License (MIT)
# Copyright (c) 2016, Peter Jaffe
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in the
# Software without restriction, including without limitation the rights to use, copy,
# modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the
# following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Author: Peter Jaffe, pjaffe@hearst.com
# Date: 2016-08-04

import json, time, requests

# Turn one or both getLoggers on to get some detailed logging of your requests and responses
#import logging
#logging.basicConfig(level=logging.INFO)
#logging.getLogger('suds.transport').setLevel(logging.DEBUG)
#logging.getLogger('suds.client').setLevel(logging.DEBUG)

# Adbook API is SOAP, so we will use the suds SOAP library.
from suds.client import Client

# Adbook has two API subdomains. The endpoints seem to be identical, but you have to use
# the subdomain that has been assigned to you. Your user will not be able to authenticate otherwise.
# So in the below, substitute 'adbook' for 'adserver' in the path if that is appropriate -
# check with your adbook rep if unsure which subdomain to use.

# This is the service endpoint:
# http://adserver.fattail.com/abn/ws/AdBookConnect.svc

# This WSDL is .NET, so there are both a recursive and a flat version available.
# Neither the python suds nor zeep library can import the recursive wsdl:
# http://adserver.fattail.com/abn/ws/AdBookConnect.svc?wsdl
# So use the flat wsdl:
wsdl = 'http://adserver.fattail.com/abn/ws/AdBookConnect.svc?singleWsdl'
client = Client(wsdl)

# The API uses wsse security, which is explained here: http://www.herongyang.com/Web-Services/WS-Security-Username-Token-Profile.html
# Although the Adbook docs I've seen indicate that a nonce and created should be supplied, the API uses the PasswordText implementation
# (it connects over https), so it's just simple username & password in a UsernameToken object type.
# Security() is from suds.wsse
from suds.wsse import *
security = Security()
# Read the username and password from a file. Format is simply json:
# { "username": "YOUR_USERNAME", "password": "YOUR_PASSWORD" }
# Feel free to get fancier with this if you like.
with open("auth.json","rb") as f:
    auth = json.load(f)
token = UsernameToken(auth['username'],auth['password'])
security.tokens.append(token)
client.set_options(wsse=security)

# This script assumes there is a saved report already created in the Adbook UI.
# To get the list of saved reports:
sr = client.service.GetSavedReportList()

# If you just want to see the available reports:
for SavedReport in sr['SavedReport']:
    print "Name: {0}, ID: {1}".format(SavedReport['Name'],SavedReport['SavedReportID'])

# To get the parameters for a specific report - for example, the first report in the SavedReportList
reportId = sr['SavedReport'][0]['SavedReportID']
qr = client.service.GetSavedReportQuery(reportId)

# This returns an object containing the name, report ID, and the query used to generate the report.
# The query is itelf an object made up of various elements such as the output column list, the metrics, the filters, etc.
# The query is contained in an object called 'ReportQuery' at the top level of the response.

# You may be able to skip the next steps and simply take the ReportQuery from the savedReportQuery response.
# If the report template you're using has not produced any empty elements, then simply doing:
ReportQuery = qr['ReportQuery']
# should work. However, if for example your template query does not have any value in DeliveryDetailOutputColumnIDList,
# then qr['ReportQuery'] will include:
#    DeliveryDetailOutputColumnIDList = None
# This will cause your report job submission to fail because the element will not be passed by suds but is required.
# Pass any elements that don't have values in your template (IE in qr['ReportQuery']), as well as the correct index for that element,
# to the addElements class defined below, and pass to client.options.plugins.
# Check the index by looking at the order of elements in ReportQuery. Basically,
# DeliveryDetailOutputColumnIDList = 0
# FieldOutputColumnNameList = 1
# MetricOutputColumnIDList = 2
# QueryFilterList = 3
# QueryParameterList = 4

from suds.plugin import MessagePlugin
from suds.sax.element import Element

class addElements(MessagePlugin):
    def __init__(self, *args):
        self.args = args
    def marshalled(self,context):
        ReportQuery = context.envelope.getChild('Body').getChild('RunReportJob').getChild('reportJob').getChild('ReportQuery')
        for elem in self.args:
            try:
                print "Adding element to ReportQuery: %s" % elem[0]
                e = Element(elem[0])
                i = elem[1]
                ReportQuery.insert(e,index=i)
            except Exception, m:
                print "Unable to insert %s to ReportQuery." % elem[0]
                print m

# In each case you need to append the namespace (ns1) at the beginning.
# If you are passing multiple elements, note that each one must be passed as an array,
# E.G., client.options.plugins = [addElements(['ns1:element1',0],['ns1:element3',2])]
client.options.plugins = [addElements(['ns1:DeliveryDetailOutputColumnIDList',0])] # comment this out if you have a DeliveryDetailOutputColumnIDList!


# If you need to change any parameters of the query - such as start and end date - they can simply be edited in the object.
# Example of editing start and end date:
from datetime import timedelta, date
startdate = (date.today()-timedelta(7)).strftime('%Y-%m-%d')
enddate = (date.today()-timedelta(1)).strftime('%Y-%m-%d')

for qp in ReportQuery['QueryParameterList']['QueryParameter']:
    if qp['ParameterType'] == 'StartDate':
        qp['ParameterValue'] = startdate
    elif qp['ParameterType'] == 'EndDate':
        qp['ParameterValue'] = enddate


# Next get a ReportJob object. client.factory.create() generates a suds object with the
# appropriate schema but empty values.
ReportJob=client.factory.create('ReportJob')

# Put the ReportQuery into the ReportJob
ReportJob.ReportQuery = ReportQuery

# The ReportJob includes several things in addition to the ReportQuery: ReportJobId, Status, and StatusMessage.
# Each of those will be returned from the service; you do not want to submit any of them.
# Suds will pass complex types even if their values are empty.
# Status is a complex type, so it will be passed, which will cause an error with the service.
# So we want to kill it by setting it to None so it does not get passed.
ReportJob.Status = None

# Now we can pass the ReportJob and get a job ID
rj = client.service.RunReportJob(ReportJob)
jobId = rj.ReportJobID

# If you added the plugins above, clear the plugins to avoid an annoying error message
client.options.plugins = []

# Check to see if the report job is done running
done = False
while not done:
    status = client.service.GetReportJob(jobId).Status
    if status == "Done":
        done = True
    else:
        print "Job %i status: %s, sleeping 10 seconds" % (jobId,status)
        time.sleep(10)

# Now to download the report.
# Disable the annoying insecure platform warning from urllib3
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

def download_file(url,output_filename):
    r = requests.get(url, stream=True)
    with open(output_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

# Request the download URL for the report
dlUrl = client.service.GetReportDownloadURL(jobId,"CSV")

# Make the output name whatever you want
output_filename = "adbook.csv"

download_file(dlUrl,output_filename)
print "File downloaded to %s" % output_filename
