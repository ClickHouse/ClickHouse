#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# `async_insert` into a DeltaLake table: every flushed batch is one commit and all acknowledged
# rows are readable; the writes setting of the inserting session governs the flush.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_async"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-async","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

versions() {
    echo "versions: $(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
}

$CLICKHOUSE_CLIENT --query "CREATE TABLE dl (id Int32) ENGINE = DeltaLakeLocal('${TABLE}')"

echo "-- async inserts, each waited for: acknowledged rows are committed and readable"
for i in 1 2 3; do
    $CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --async_insert=1 --wait_for_async_insert=1 --query "INSERT INTO dl VALUES (${i})"
done
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(id) FROM dl"
versions

echo "-- async insert with the writes setting off is rejected and commits nothing"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --async_insert=1 --wait_for_async_insert=1 --query "INSERT INTO dl VALUES (4)" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | head -1
$CLICKHOUSE_CLIENT --query "SELECT count() FROM dl"
versions

echo "-- async insert with the writes setting off and no wait: nothing is committed"
echo "-- (the INSERT itself is still acknowledged, https://github.com/ClickHouse/ClickHouse/issues/120717)"
NOWAIT_QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_nowait_$(random_str 8)"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --async_insert=1 --wait_for_async_insert=0 --async_insert_use_adaptive_busy_timeout=0 --async_insert_busy_timeout_ms=600000 --query_id="${NOWAIT_QUERY_ID}" --query "INSERT INTO dl VALUES (5)" 2>/dev/null
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH ASYNC INSERT QUEUE dl" 2>/dev/null
$CLICKHOUSE_CLIENT --query "SELECT count() FROM dl"
versions
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS asynchronous_insert_log;
    SELECT 'flush errors with SUPPORT_IS_DISABLED for the no-wait insert:', count() FROM system.asynchronous_insert_log
    WHERE query_id = '${NOWAIT_QUERY_ID}' AND status = 'FlushError' AND exception LIKE '%SUPPORT_IS_DISABLED%';
"

echo "-- two concurrent async inserts with identical settings both land (one or two flushes)"
ASYNC="--allow_delta_lake_writes=1 --async_insert=1 --wait_for_async_insert=1 --async_insert_busy_timeout_min_ms=3000 --async_insert_busy_timeout_max_ms=3000"
$CLICKHOUSE_CLIENT ${ASYNC} --query "INSERT INTO dl VALUES (10)" &
$CLICKHOUSE_CLIENT ${ASYNC} --query "INSERT INTO dl VALUES (11)" &
wait
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(id) FROM dl"
VERSIONS=$(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))
echo "versions in [4, 5]: $(( VERSIONS >= 4 && VERSIONS <= 5 ))"

echo "-- a queued insert (no wait) is flushed later with the inserting session's setting, even when"
echo "-- the session that triggers the flush has writes off"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --async_insert=1 --wait_for_async_insert=0 --async_insert_use_adaptive_busy_timeout=0 --async_insert_busy_timeout_ms=600000 --query "INSERT INTO dl VALUES (20)"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --query "SYSTEM FLUSH ASYNC INSERT QUEUE dl"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(id) FROM dl"
VERSIONS_AFTER=$(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))
echo "one more version after the explicit flush: $(( VERSIONS_AFTER == VERSIONS + 1 ))"

$CLICKHOUSE_CLIENT --query "DROP TABLE dl"
