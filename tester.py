import redis
import socket

if False:
    r = redis.Redis(host='127.0.0.1', port=9006, ssl=True, ssl_cert_reqs=None)
    #resp = r.get('value')
    #print(resp)

    resp = r.select(1)
    print(resp)

    resp = r.auth('password')
    print(resp)

    resp = r.auth('login', 'password')
    print(resp)
else:
    addr = ('127.0.0.1', 9006)
    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM)
    sock.connect(addr)

    sock.send(b'*3\r\n$4\r\nAUTH\r\n$3\r\nlol\r\n$3\r\nlol\r\n')

    resp = sock.recv(4098)
    print(resp)

