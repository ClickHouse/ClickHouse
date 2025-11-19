#!/usr/bin/env bash
# Tags: no-random-settings, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "select throwIf(number > 10000000) + number check from numbers(10000005) format CSV" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&query=&enable_http_compression=1" -H "Accept-Encoding: zstd" --compressed --data-binary @- | grep -q "__exception__" && echo 'zstd OK'
echo "select throwIf(number > 10000000) + number check from numbers(10000005) format CSV" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&query=&enable_http_compression=1" -H "Accept-Encoding: gzip" --compressed --data-binary @- | grep -q "__exception__" && echo 'gzip OK'
echo "select throwIf(number > 10000000) + number check from numbers(10000005) format CSV" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&query=&enable_http_compression=1" -H "Accept-Encoding: br" --compressed --data-binary @- | grep -q "__exception__" && echo 'br OK'
echo "select throwIf(number > 10000000) + number check from numbers(10000005) format CSV" | ${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&query=&enable_http_compression=1" -H "Accept-Encoding: deflate" --compressed --data-binary @- | grep -q "__exception__" && echo 'deflate OK'
