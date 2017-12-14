#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

curl -vsS 'http://localhost:8123/?add_http_cors_header=1' -H "Origin:smi2.ru"  --data-binary @- <<< "SELECT 1" 2>&1 | grep -F "< Access-Control-Allow-Origin: *" | wc -l
curl -vsS 'http://localhost:8123/?add_http_cors_header=0' -H "Origin:smi2.ru"  --data-binary @- <<< "SELECT 1" 2>&1 | grep -F "< Access-Control-Allow-Origin: *" | wc -l
curl -vsS 'http://localhost:8123/?add_http_cors_header=1' --data-binary @- <<< "SELECT 1" 2>&1 | grep -F "< Access-Control-Allow-Origin: *" | wc -l
