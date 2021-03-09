#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1"                                     -d 'SELECT number FROM system.numbers LIMIT 10';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=0" -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 10';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d;
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: gzip, deflate' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d;
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: zip, eflate'   -d 'SELECT number FROM system.numbers LIMIT 10';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: br'            -d 'SELECT number FROM system.numbers LIMIT 10' | brotli -d;
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: xz'            -d 'SELECT number FROM system.numbers LIMIT 10' | xz -d;

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1"                                     -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: deflate'       -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: gzip, deflate' -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: zip, eflate'   -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: br'            -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: xz'            -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep --text '< Content-Encoding';

echo "SELECT 1" | ${CLICKHOUSE_CURL} -sS --data-binary @- "${CLICKHOUSE_URL}";
echo "SELECT 1" | gzip -c | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: gzip' "${CLICKHOUSE_URL}";
echo "SELECT 1" | brotli | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: br' "${CLICKHOUSE_URL}";
echo "SELECT 1" | xz -c | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: xz' "${CLICKHOUSE_URL}";

echo "'Hello, world'" | ${CLICKHOUSE_CURL} -sS --data-binary @- "${CLICKHOUSE_URL}&query=SELECT";
echo "'Hello, world'" | gzip -c | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: gzip' "${CLICKHOUSE_URL}&query=SELECT";
echo "'Hello, world'" | brotli | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: br' "${CLICKHOUSE_URL}&query=SELECT";
echo "'Hello, world'" | xz -c | ${CLICKHOUSE_CURL} -sS --data-binary @- -H 'Content-Encoding: xz' "${CLICKHOUSE_URL}&query=SELECT";

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_http_compression=1" -H 'Accept-Encoding: gzip'          -d 'SELECT number FROM system.numbers LIMIT 0' | wc -c;

# POST multiple concatenated gzip streams.
(echo -n "SELECT 'Part1" | gzip -c; echo " Part2'" | gzip -c) | ${CLICKHOUSE_CURL} -sS -H 'Content-Encoding: gzip' "${CLICKHOUSE_URL}" --data-binary @-
