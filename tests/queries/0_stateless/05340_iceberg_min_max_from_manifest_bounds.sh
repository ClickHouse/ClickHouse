#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `min`, `max`, `count()` and `count(c)` over an Iceberg table are answered from the column bounds
# and null counts in the manifest files when the setting `optimize_trivial_min_max_query` is enabled.
# Every query is run with the optimization enabled and disabled; the results must match. The third
# line of each block tells whether the plan was served from the metadata (1) or read the data (0).

TABLE="t_${CLICKHOUSE_DATABASE}_min_max"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer=1 --allow_insert_into_iceberg=1 --session_timezone=UTC"

# Background Iceberg compaction would rewrite the manifests, so it is pinned off for the table.
${CLIENT} --query "
    CREATE TABLE ${TABLE} (i Int32, l Int64, d Date32, ts DateTime64(6, 'UTC'), s String, n Nullable(Int64))
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS allow_experimental_iceberg_compaction = 0
"

check()
{
    local query=$1
    ${CLIENT} --optimize_trivial_min_max_query=1 --query "${query}"
    ${CLIENT} --optimize_trivial_min_max_query=0 --query "${query}"
    ${CLIENT} --optimize_trivial_min_max_query=1 --query "EXPLAIN ${query}" | grep -c "Optimized trivial min/max" || true
}

QUERY="SELECT min(i), max(i), min(l), max(l), min(d), max(d), min(ts), max(ts), min(n), max(n), count(), count(n) FROM ${TABLE}"

echo '--- empty table ---'
check "${QUERY}"

# Three data files; the last one holds only NULL in `n`.
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (5, 50, '2020-01-05', '2020-01-05 00:00:00.000005', 'e', 500), (3, 30, '2020-01-03', '2020-01-03 00:00:00.000003', 'c', NULL)"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (-7, -70, '1960-01-01', '1960-01-01 00:00:00.000007', 'g', -700), (9, 90, '2030-01-09', '2030-01-09 00:00:00.000009', 'i', 900)"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (1, 10, '2020-01-01', '2020-01-01 00:00:00.000001', 'a', NULL)"

echo '--- served from the manifests ---'
check "${QUERY}"
check "SELECT count(n) FROM ${TABLE}"
check "SELECT max(i), min(i), max(i) + 1 FROM ${TABLE} HAVING min(i) < 0"

echo '--- not applied: String column, WHERE, GROUP BY, other aggregates ---'
check "SELECT min(s), max(s) FROM ${TABLE}"
check "SELECT min(i) FROM ${TABLE} WHERE l > 0"
check "SELECT l % 2 AS k, min(i) FROM ${TABLE} GROUP BY k ORDER BY k"
check "SELECT min(i), sum(l) FROM ${TABLE}"

echo '--- not applied once the snapshot has delete files ---'
${CLIENT} --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE i = 9"
check "${QUERY}"
