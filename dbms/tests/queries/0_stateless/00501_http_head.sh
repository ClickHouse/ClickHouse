#!/usr/bin/env bash

curl -s --max-time 0.5 -X "HEAD" 'http://localhost:8123/?query=SELECT%201'
curl -s --max-time 0.5 -X "HEAD" 'http://localhost:8123/?query=select+*+from+system.numbers+limit+1000000'
