from bottle import request, route, run, response, HTTPError

### AWS zone API mock


@route("/latest/api/token", ["PUT"])
def api_token():
    return "manually_crafted_token"


@route("/latest/meta-data/placement/availability-zone-id")
def placement_availability_zone_id():
    # raise HTTPError(403, "Permission Denied")
    return "euc1-az2"


@route("/ping")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
