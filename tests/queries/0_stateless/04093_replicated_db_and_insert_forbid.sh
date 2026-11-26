#!/usr/bin/env bash
# Tags: replica, zookeeper

# Test for CREATE TABLE IF NOT EXISTS ... AND INSERT restrictions with ReplicatedMergeTree
# in a Replicated database.

set -e -o pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query \
    "CREATE DATABASE ${CLICKHOUSE_DATABASE}_rmt_db ENGINE = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_rmt_db', '{shard}', '{replica}')" || exit 1

trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db"' EXIT

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = ReplicatedMergeTree ORDER BY a" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 1 AS a, 'test' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query \
    "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 2 AS a, 'test2' AS b; -- { serverError SUPPORT_IS_DISABLED }" || exit 1

${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table" || exit 1

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS t_04093_stream;
        DROP TABLE IF EXISTS t_04093_http;
        DROP TABLE IF EXISTS t_04093_http_input;
        DROP TABLE IF EXISTS t_04093_large;
        DROP TABLE IF EXISTS t_04093_concurrent;
        DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_rmt_db;
    "
}
trap cleanup EXIT
${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP TABLE IF EXISTS t_04093_stream;
    DROP TABLE IF EXISTS t_04093_http;
    DROP TABLE IF EXISTS t_04093_http_input;
    DROP TABLE IF EXISTS t_04093_large;
    DROP TABLE IF EXISTS t_04093_concurrent;
"

printf '1\n2\n99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE t_04093_stream (x UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV"
printf '3\n4\n99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_stream (x UInt8) ENGINE = Memory AND INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

printf '99\n' | ${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_stream AS INSERT SELECT * FROM input('auto') FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04093_stream VALUES (5)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_stream AS INSERT SELECT * FROM input('auto') FORMAT CSV" \
    '99' | ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_04093_stream VALUES (6)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_stream"

printf '%s\n' \
    'CREATE TABLE t_04093_http (x String) ENGINE = Memory AS INSERT FORMAT CSV' \
    'SETTINGS made_up_setting = 1' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_04093_http"

printf '%s\n' \
    "CREATE TABLE t_04093_http_input (x UInt8) ENGINE = Memory AS INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV" \
    '1' '2' '99' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
printf '%s\n' \
    "CREATE TABLE IF NOT EXISTS t_04093_http_input (x UInt8) ENGINE = Memory AND INSERT SELECT * FROM input('x UInt8') SETTINGS limit = 2 FORMAT CSV" \
    '3' '4' '99' | \
    ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_http_input"

{
    printf 'CREATE TABLE t_04093_large (x UInt8) ENGINE = Memory AS INSERT FORMAT RowBinary\n'
    head -c 10000 /dev/zero
} | ${CLICKHOUSE_CURL} -sS --fail-with-body "${CLICKHOUSE_URL}&max_query_size=1024" --data-binary @-
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_04093_large"

${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_concurrent (x UInt8) ENGINE = MergeTree ORDER BY x AND INSERT SELECT 1" &
first_pid=$!
${CLICKHOUSE_CLIENT} --query \
    "CREATE TABLE IF NOT EXISTS t_04093_concurrent (x UInt8) ENGINE = MergeTree ORDER BY x AND INSERT SELECT 2" &
second_pid=$!
status=0
wait "$first_pid" || status=1
wait "$second_pid" || status=1
if [ "$status" -ne 0 ]; then
    exit "$status"
fi
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM t_04093_concurrent"