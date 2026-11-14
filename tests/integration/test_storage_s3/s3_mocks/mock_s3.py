import sys

from bottle import request, response, route, run


@route("/redirected/<_path:path>")
def infinite_redirect(_path):
    response.set_header("Location", request.url)
    response.status = 307
    return "Redirected"


@route("/tagging-denied/<_path:path>")
def tagging_denied(_path):
    if "tagging" in request.query_string:
        result = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>'
        response.status = 403
        response.content_type = "application/xml"
        response.set_header("Content-Length", len(result))
        return result
    result = "1\n"
    response.content_type = "text/plain"
    response.set_header("Content-Length", len(result))
    response.set_header("ETag", '"x"')
    response.set_header("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
    response.set_header("x-amz-tagging-count", "1")
    return result


@route("/<_bucket>/<_path:path>")
def server(_bucket, _path):
    for name in request.headers:
        if name == "Authorization" and request.headers[name] == "Bearer TOKEN":
            result = "1, 2, 3"
            response.content_type = "text/plain"
            response.set_header("Content-Length", len(result))
            return result

    result = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>ForbiddenError</Code><Message>Forbidden Error</Message><RequestId>txfbd566d03042474888193-00608d7537</RequestId></Error>'
    response.status = 403
    response.content_type = "text/xml"
    response.set_header("Content-Length", len(result))
    return result


@route("/")
def ping():
    response.content_type = "text/plain"
    response.set_header("Content-Length", 2)
    return "OK"


run(host="0.0.0.0", port=int(sys.argv[1]))
