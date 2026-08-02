#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS numbers";
$CLICKHOUSE_CLIENT --query="CREATE TABLE numbers (number UInt64) engine = MergeTree order by number";
$CLICKHOUSE_CLIENT --query="INSERT INTO numbers select * from system.numbers limit 10";

$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
$CLICKHOUSE_CLIENT --query="SELECT number FROM numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

# use_query_cache=0 - to ensure that results are not cached and we always get the proper statistics for number of rows read
# http_wait_end_of_query=1 - to ensure that the query statistics are sent after the query is fully executed
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?use_query_cache=0&http_wait_end_of_query=1" -d "SELECT number FROM numbers LIMIT 10 FORMAT JSON" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?use_query_cache=0&http_wait_end_of_query=1" -d "SELECT number FROM numbers LIMIT 10 FORMAT JSONCompact" | grep 'rows_read';
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?use_query_cache=0&http_wait_end_of_query=1" -d "SELECT number FROM numbers LIMIT 10 FORMAT XML" | grep 'rows_read';

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS numbers";
