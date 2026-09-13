#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The `written_rows` quota resource limits the number of rows written by INSERTs, the same way
# `written_bytes` limits the bytes. It is accounted in the counting transform of the insert
# pipeline, so it covers synchronous and asynchronous inserts as well as INSERT ... SELECT.
# Quotas, users and roles are server-global, so the names are made unique.

ROLE="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
QUOTA="q_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS written_rows_05060"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE written_rows_05060 (s String) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${ROLE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "GRANT ${ROLE} TO ${USER}"

echo "-- both spellings of the resource are accepted"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX WRITTEN ROWS = 5 TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE QUOTA ${QUOTA}" | sed "s/${QUOTA}/q/; s/${ROLE}/r/"
${CLICKHOUSE_CLIENT} -q "ALTER QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX written_rows = 5"
${CLICKHOUSE_CLIENT} -q "SELECT max_written_rows FROM system.quota_limits WHERE quota_name = '${QUOTA}'"

echo "-- 5 rows allowed: 2 (async) + 2 (sync) fit, the third insert of 2 rows exceeds and writes nothing"
${CLICKHOUSE_CLIENT} --user ${USER} --async_insert 1 -q "INSERT INTO written_rows_05060 VALUES ('a'), ('b')"
${CLICKHOUSE_CLIENT} --user ${USER} --async_insert 0 -q "INSERT INTO written_rows_05060 VALUES ('c'), ('d')"
${CLICKHOUSE_CLIENT} --user ${USER} --async_insert 1 -q "INSERT INTO written_rows_05060 VALUES ('e'), ('f')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
echo "-- once exceeded, the next inserts are rejected before writing anything"
${CLICKHOUSE_CLIENT} --user ${USER} --async_insert 0 -q "INSERT INTO written_rows_05060 VALUES ('g')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
${CLICKHOUSE_CLIENT} --user ${USER} --async_insert 1 -q "INSERT INTO written_rows_05060 VALUES ('h')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
${CLICKHOUSE_CLIENT} -q "SELECT written_rows, max_written_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM written_rows_05060"

echo "-- INSERT SELECT: 50 rows fit into 100, the next 100 do not"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX written_rows = 100 TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE written_rows_05060"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO written_rows_05060 SELECT toString(number) FROM numbers(50)"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO written_rows_05060 SELECT toString(number) FROM numbers(100)" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
${CLICKHOUSE_CLIENT} -q "SELECT written_rows, max_written_rows FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM written_rows_05060"

echo "-- written_rows and written_bytes are independent counters"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA ${QUOTA}"
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${QUOTA} FOR INTERVAL 100 YEAR MAX written_rows = 3, written_bytes = 1000000 TO ${ROLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE written_rows_05060"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO written_rows_05060 VALUES ('a long string that costs many bytes but only one row')"
${CLICKHOUSE_CLIENT} --user ${USER} -q "INSERT INTO written_rows_05060 VALUES ('x'), ('y'), ('z')" 2>&1 | grep -m1 -o QUOTA_EXCEEDED
${CLICKHOUSE_CLIENT} -q "SELECT written_rows, max_written_rows, written_bytes < max_written_bytes FROM system.quotas_usage WHERE quota_name = '${QUOTA}'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM written_rows_05060"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS written_rows_05060"
${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS ${ROLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${QUOTA}"
