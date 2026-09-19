from threading import Lock

from bottle import request, response, route, run

# Endpoint can be configured to throw 500 error on N-th request attempt.
# In usual situation just redirects to original Minio server.

# Dict to the number of request should be failed.
cache = {}
mutex = Lock()


@route("/fail_request/<_request_number>")
@route("/fail_request/<_request_number>/<_method>")
def fail_request(_request_number, _method=None):
    request_number = int(_request_number)
    if request_number > 0:
        cache["request_number"] = request_number
        cache["fail_method"] = _method.upper() if _method else None
        # Optional query parameters; the defaults reproduce a single 500 ExpectedError.
        cache["fail_status"] = int(request.query.get("status") or 500)
        cache["fail_code"] = request.query.get("code") or "ExpectedError"
        cache["fail_repeat"] = int(request.query.get("repeat") or 1)
    else:
        cache.pop("request_number", None)
        cache.pop("fail_method", None)
        cache.pop("fail_status", None)
        cache.pop("fail_code", None)
        cache.pop("fail_repeat", None)
    return "OK"


@route("/throttle_request/<_request_number>")
def throttle_request(_request_number):
    request_number = int(_request_number)
    if request_number > 0:
        cache["throttle_request_number"] = request_number
    else:
        cache.pop("throttle_request_number", None)
    return "OK"


# Handle for MultipleObjectsDelete.
@route("/<_bucket>", ["POST"])
def delete(_bucket):
    response.set_header(
        "Location", "http://minio1:9001/" + _bucket + "?" + request.query_string
    )
    response.status = 307
    return "Redirected"


@route("/<_bucket>/<_path:path>", ["GET", "POST", "PUT", "DELETE"])
def server(_bucket, _path):
    # It's delete query for failed part
    if _path.endswith("delete"):
        response.set_header("Location", "http://minio1:9001/" + _bucket + "/" + _path)
        response.status = 307
        return "Redirected"

    mutex.acquire()
    try:
        fail_method = cache.get("fail_method", None)
        if cache.get("request_number", None) and (
            fail_method is None or request.method == fail_method
        ):
            request_number = cache.pop("request_number") - 1
            if request_number > 0:
                cache["request_number"] = request_number
            else:
                # Once the counter reaches the target request, fail it `repeat` times in a
                # row (repeat=1 clears the fault immediately, as it always did).
                repeat = cache.get("fail_repeat", 1) - 1
                if repeat > 0:
                    cache["request_number"] = 1
                    cache["fail_repeat"] = repeat
                else:
                    cache.pop("fail_method", None)
                    cache.pop("fail_repeat", None)
                response.status = cache.get("fail_status", 500)
                response.content_type = "text/xml"
                return '<?xml version="1.0" encoding="UTF-8"?><Error><Code>{}</Code><Message>Expected Error</Message><RequestId>txfbd566d03042474888193-00608d7537</RequestId></Error>'.format(
                    cache.get("fail_code", "ExpectedError")
                )

        if cache.get("throttle_request_number", None):
            request_number = cache.pop("throttle_request_number") - 1
            if request_number > 0:
                cache["throttle_request_number"] = request_number
            else:
                response.status = 429
                response.content_type = "text/xml"
                return '<?xml version="1.0" encoding="UTF-8"?><Error><Code>TooManyRequestsException</Code><Message>Please reduce your request rate.</Message><RequestId>txfbd566d03042474888193-00608d7538</RequestId></Error>'
    finally:
        mutex.release()

    response.set_header("Location", "http://minio1:9001/" + _bucket + "/" + _path)
    response.status = 307
    return "Redirected"


@route("/")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
