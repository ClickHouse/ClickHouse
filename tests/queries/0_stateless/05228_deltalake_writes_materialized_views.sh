#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Writes through a regular MV (setting must be on the inserting session) and a refreshable
# `APPEND TO` MV (setting in the view's SETTINGS clause); one version per push, fail closed otherwise.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_mv"
TABLE="${ROOT}/t"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

mkdir -p "${TABLE}/_delta_log"
cat > "${TABLE}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-mv","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF

versions() {
    echo "versions: $(($(find "${TABLE}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
}

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE src (id Int64, s String) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dl (id Int64, s String) ENGINE = DeltaLakeLocal('${TABLE}');
    CREATE MATERIALIZED VIEW mv TO dl AS SELECT id, s FROM src;
"

echo "-- regular MV: each INSERT into the source is one Delta version"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query "INSERT INTO src SELECT number, toString(number) FROM numbers(5)"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query "INSERT INTO src SELECT number, toString(number) FROM numbers(5, 5)"
versions
$CLICKHOUSE_CLIENT --query "SELECT count(), min(id), max(id) FROM dl"

echo "-- regular MV with the writes setting off: the source INSERT fails as a whole, nothing committed"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --query "INSERT INTO src VALUES (100, 'x')" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | head -1
versions
$CLICKHOUSE_CLIENT --query "SELECT count() AS src_rows FROM src; SELECT count() AS delta_rows FROM dl"

echo "-- regular MV: a SETTINGS clause in the view definition applies to its SELECT only, the push into"
echo "-- the Delta target still runs with the inserting session's settings (so it is still rejected)"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE mv;
    CREATE MATERIALIZED VIEW mv TO dl AS SELECT id, s FROM src SETTINGS allow_delta_lake_writes = 1;
"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --query "INSERT INTO src VALUES (100, 'x')" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | head -1
versions
$CLICKHOUSE_CLIENT --query "SELECT count() AS src_rows FROM src; SELECT count() AS delta_rows FROM dl"

# enable_parallel_replicas = 0 in the refresh definitions: https://github.com/ClickHouse/ClickHouse/issues/120714
echo "-- refreshable MV APPEND TO delta with the setting off in its definition: the refresh fails, nothing committed"
$CLICKHOUSE_CLIENT --query "
    CREATE MATERIALIZED VIEW rmv_nosetting REFRESH EVERY 100 YEAR SETTINGS refresh_retries = 0 APPEND TO dl
    AS SELECT id + 1000 AS id, s FROM src SETTINGS allow_delta_lake_writes = 0, enable_parallel_replicas = 0;
    SYSTEM REFRESH VIEW rmv_nosetting;
    SYSTEM WAIT VIEW rmv_nosetting;
" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | head -1
$CLICKHOUSE_CLIENT --query "SELECT last_success_time IS NULL, exception LIKE '%SUPPORT_IS_DISABLED%' FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'rmv_nosetting'"
versions

echo "-- refreshable MV APPEND TO delta with the setting on in its definition: one version per refresh,"
echo "-- triggered from a session that has writes off"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --query "
    CREATE MATERIALIZED VIEW rmv REFRESH EVERY 100 YEAR SETTINGS refresh_retries = 0 APPEND TO dl
    AS SELECT id + 1000 AS id, s FROM src SETTINGS allow_delta_lake_writes = 1, enable_parallel_replicas = 0;
    SYSTEM REFRESH VIEW rmv;
    SYSTEM WAIT VIEW rmv;
    SYSTEM REFRESH VIEW rmv;
    SYSTEM WAIT VIEW rmv;
"
versions
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM dl WHERE id >= 1000;
    SELECT status, exception FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'rmv';
"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE rmv;
    DROP TABLE rmv_nosetting;
    DROP TABLE mv;
    DROP TABLE dl;
    DROP TABLE src;
"
