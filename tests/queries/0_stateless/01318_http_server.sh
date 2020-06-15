#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

echo -n -e "POST / HTTP/1.1\r\nContent-Length: 8\r\n\r\nSELECT 1" | nc localhost 8123 | sed -r -e 's/((Date|X-ClickHouse-(Timezone|Query-Id|Server-Display-Name)): ).+$/\1/; /0\r/q'
echo -n -e "GET /?query=SELECT+1 HTTP/1.1\r\nHost: localhost:8123\r\n\r\n" | nc localhost 8123 | sed -r -e 's/((Date|X-ClickHouse-(Timezone|Query-Id|Server-Display-Name)): ).+$/\1/; /0\r/q'
echo -n -e "POST / HTTP/1.0\r\nContent-Length: 8\r\n\r\nSELECT 1" | nc localhost 8123 | sed -r -e 's/((Date|X-ClickHouse-(Timezone|Query-Id|Server-Display-Name)): ).+$/\1/'
