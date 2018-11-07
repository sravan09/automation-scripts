#!/usr/bin/python
"""
Author: Sravan Kumar Tammineni
Description: Couchbase CE doesn't support auto purge of expired docs(TTL). So this tool hepls to delete documents older than specified number of days.
Pre-requisite: Should have secondary index created on epoch time field in the bucket.
Tested on Centos
Help: ./couchbase_purge.py -h
"""
import time
import argparse
import os

def purge_docs(url, username, password, index_field, bucket, days, timeout):
    epoch_time = (int(time.time())-days*86400)*1000
    query = "/opt/couchbase/bin/cbq -engine={0} --timeout={1}h -u {2} -p {3} --script='delete from {4} where {5} < {6};'".format(url,timeout,username,password,bucket,index_field,epoch_time)
    os.system(query)
    print "Data deleted older than {0} days from {1} bucket".format(days,bucket)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='purge old data')
        optional = parser.add_argument_group('optional arguments')
        required = parser.add_argument_group('required arguments')
        required.add_argument('-u','--username', help="user name", required=True)
        required.add_argument('-p','--password', help="password", required=True)
        required.add_argument('-i','--index_field', help="index field", required=True)
        optional.add_argument('-P','--port', help="port", default='8091')
        optional.add_argument('-b','--bucket', help="bucket name", default="beer-sample")
        optional.add_argument('-d','--days', type=int, help="number of days", default='15')
        optional.add_argument('-t','--timeout', type=int, help="query timeout in hours", default='1')
        args = parser.parse_args()
        bucket = args.bucket
        days = args.days
        username = args.username
        password = args.password
        index_field = args.index_field
        port = args.port
        timeout = args.timeout
        host = os.uname()[1]
        url = ''.join(['http://', host, ':', port])
        purge_docs(url, username, password, index_field, bucket, days, timeout)
    except KeyboardInterrupt:
        print 'Ctrl^C Caught! Exiting...'
        sys.exit(-1)
