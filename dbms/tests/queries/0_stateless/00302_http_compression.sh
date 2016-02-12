#!/bin/bash

curl -sS 'http://localhost:8123/'                                     -d 'SELECT number FROM system.numbers LIMIT 10';
curl -sS 'http://localhost:8123/' -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d;
curl -sS 'http://localhost:8123/' -H 'Accept-Encoding: gzip, deflate' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d;
curl -sS 'http://localhost:8123/' -H 'Accept-Encoding: zip, eflate'   -d 'SELECT number FROM system.numbers LIMIT 10';

curl -vsS 'http://localhost:8123/'                                     -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
curl -vsS 'http://localhost:8123/' -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
curl -vsS 'http://localhost:8123/' -H 'Accept-Encoding: deflate'       -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
curl -vsS 'http://localhost:8123/' -H 'Accept-Encoding: gzip, deflate' -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
curl -vsS 'http://localhost:8123/' -H 'Accept-Encoding: zip, eflate'   -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';

echo "SELECT 1" | curl -sS --data-binary @- 'http://localhost:8123/';
echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/';

echo "'Hello, world'" | curl -sS --data-binary @- 'http://localhost:8123/?query=SELECT';
echo "'Hello, world'" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/?query=SELECT';
