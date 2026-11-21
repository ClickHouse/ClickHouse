#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `KEYED BY normalized_query_hash` quota must give each distinct query pattern its own `written_bytes`
# bucket for asynchronous inserts as well. The async insert flush builds `InterpreterInsertQuery` directly
# (it does not go through `executeQuery`), so it has to set the normalized query hash on the flush context
# itself; otherwise the `WRITTEN_BYTES` pre-check and the `CountingTransform` both see hash `0` and every
# async insert pattern shares a single bucket. This test uses two structurally different INSERT statements
# (the same data inserted with the `Values` and the `JSONEachRow` formats, which serialize to different
# normalized hashes) and checks that their `written_bytes` buckets are independent. It fails if the flush
# context keeps using hash `0`: pattern B would then be rejected because pattern A already exhausted the
# shared bucket. Note that the two patterns must differ structurally — quota keys do not distinguish queries
# by table or column name, so two inserts into different tables would share a single normalized hash.
#
# Quotas, users and tables are server-global, so the names are suffixed with the unique database name to
# keep the test isolated when it runs in parallel with itself (e.g. in the flaky check).

user="u_04403_${CLICKHOUSE_DATABASE}"
quota="q_04403_${CLICKHOUSE_DATABASE}"
table="t_04403_${CLICKHOUSE_DATABASE}"

# shellcheck disable=SC2086  # CLICKHOUSE_CLIENT must be word-split.
check_exceeded() { ${CLICKHOUSE_CLIENT} --user "${user}" --async_insert 1 --wait_for_async_insert 1 --send_logs_level=none -q "$1" 2>&1 | grep -m1 -o QUOTA_EXCEEDED || echo allowed; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${table} (s String) ENGINE = Memory"

echo "=== written_bytes is bucketed per normalized query hash for async inserts ==="
# Each 'qwqw' insert writes 12 bytes, so the limit of 30 bytes allows two inserts of a pattern before the
# third one exceeds its own bucket (the same calibration as 02247_written_bytes_quota).
${CLICKHOUSE_CLIENT} -q "CREATE QUOTA ${quota} KEYED BY normalized_query_hash FOR INTERVAL 100 YEAR MAX written_bytes = 30 TO ${user}"

echo "--- pattern A (Values), first insert is within the per-pattern limit ---"
check_exceeded "INSERT INTO ${table} VALUES ('qwqw')"
echo "--- pattern A, second insert is still within its own bucket ---"
check_exceeded "INSERT INTO ${table} VALUES ('qwqw')"
echo "--- pattern A, third insert exceeds its own bucket ---"
check_exceeded "INSERT INTO ${table} VALUES ('qwqw')"
echo "--- pattern B (JSONEachRow, a different format hence a different hash) has its own bucket ---"
check_exceeded "INSERT INTO ${table} FORMAT JSONEachRow {\"s\":\"qwqw\"}"
echo "--- pattern B, second insert is still within its own bucket ---"
check_exceeded "INSERT INTO ${table} FORMAT JSONEachRow {\"s\":\"qwqw\"}"
echo "--- pattern B, third insert exceeds its own bucket ---"
check_exceeded "INSERT INTO ${table} FORMAT JSONEachRow {\"s\":\"qwqw\"}"

${CLICKHOUSE_CLIENT} -q "DROP QUOTA IF EXISTS ${quota}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
