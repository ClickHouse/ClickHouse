import redis
#import socket

r = redis.Redis(host='127.0.0.1', port=9006)
resp = r.get('value')
print(resp)

#addr = ('127.0.0.1', 9006)
#sock = socket.socket(
#    socket.AF_INET,
#    socket.SOCK_STREAM)
#sock.connect(addr)

#sock.send(b'*2\r\n$3\r\nGET\r\n$8\r\nsome_key\r\n')

#resp = sock.recv(4098)
#print(resp)

