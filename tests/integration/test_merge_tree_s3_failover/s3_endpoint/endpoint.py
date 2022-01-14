from bottle import request, route, run, response


# Endpoint can be configured to throw 500 error on N-th request attempt.
# In usual situation just redirects to original Minio server.

# Dict to the number of request should be failed.
cache = {}


@route('/fail_request/<_request_number>')
def fail_request(_request_number):
    request_number = int(_request_number)
    if request_number > 0:
        cache['request_number'] = request_number
    else:
        cache.pop('request_number', None)
    return 'OK'


# Handle for MultipleObjectsDelete.
@route('/<_bucket>', ['POST'])
def delete(_bucket):
    response.set_header("Location", "http://minio1:9001/" + _bucket + "?" + request.query_string)
    response.status = 307
    return 'Redirected'


@route('/<_bucket>/<_path:path>', ['GET', 'POST', 'PUT', 'DELETE'])
def server(_bucket, _path):
    if cache.get('request_number', None):
        request_number = cache.pop('request_number') - 1
        if request_number > 0:
            cache['request_number'] = request_number
        else:
            response.status = 500
            response.content_type = 'text/xml'
            return '<?xml version="1.0" encoding="UTF-8"?><Error><Code>ExpectedError</Code><Message>Expected Error</Message><RequestId>txfbd566d03042474888193-00608d7537</RequestId></Error>'

    response.set_header("Location", "http://minio1:9001/" + _bucket + '/' + _path)
    response.status = 307
    return 'Redirected'


@route('/')
def ping():
    return 'OK'


run(host='0.0.0.0', port=8080)
