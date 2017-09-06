#!/usr/bin/env bash

( curl -v --max-time 0.3 -X "HEAD" 'http://localhost:18123/?query=SELECT%201';
  curl -v --max-time 0.3 -X "HEAD" 'http://localhost:18123/?query=select+*+from+system.numbers+limit+1000000' ) 2>&1 | grep -e "^<" | grep -v "Date:"
