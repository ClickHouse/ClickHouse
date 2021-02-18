from bottle import abort, route, run, request, response


@route('/redirected/<_path>')
def infinite_redirect(_path):
    response.set_header("Location", request.url)
    response.status = 307
    return 'Redirected'


@route('/<_bucket>/<_path>')
def server(_bucket, _path):
    for name in request.headers:
        if name == 'Authorization' and request.headers[name] == u'Bearer TOKEN':
            return '1, 2, 3'
    abort(403)


@route('/')
def ping():
    return 'OK'


run(host='0.0.0.0', port=8080)
