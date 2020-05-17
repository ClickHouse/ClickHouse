import bottle
import random


@bottle.route('/')
def index():
    if random.randrange(2) == 0:
        return 'proxy1'
    else:
        return 'proxy2'


bottle.run(host='0.0.0.0', port=8080)
