import random

import bottle


@bottle.route("/hostname")
def index():
    return "proxy1"


bottle.run(host="0.0.0.0", port=8080)
