#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database
# Incremental refreshable MV into Iceberg with `max_insert_threads > 1`: the parallel sinks of a round
# commit their data files together with the cursor in a single snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="src_${CLICKHOUSE_DATABASE}_${RANDOM}"
TGT="tgt_${CLICKHOUSE_DATABASE}_${RANDOM}"
MV="mv_${CLICKHOUSE_DATABASE}_${RANDOM}"
TGT_PATH="${USER_FILES_PATH}/${TGT}/"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${SRC} (k UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        add_minmax_index_for_block_number_column = 1,
        add_minmax_index_for_block_offset_column = 1,
        part_minmax_index_columns = 'with_block_number_offset'
"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TGT} (k UInt64)
    ENGINE = IcebergLocal('${TGT_PATH}', 'Parquet')
    PARTITION BY icebergBucket(2, k)
"

# Small blocks spread the round over all the parallel sinks.
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${MV}
        REFRESH EVERY 10 YEAR APPEND INCREMENTAL
        TO ${TGT} EMPTY
        AS SELECT k FROM ${SRC}
        SETTINGS max_insert_threads = 4, max_threads = 4, max_block_size = 100,
            min_insert_block_size_rows = 100, min_insert_block_size_bytes = 1
"

function refresh_round()
{
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${SRC} SELECT number FROM numbers($1, $2)"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW ${MV}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT VIEW ${MV}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${TGT}"
}

refresh_round 0 10000
refresh_round 10000 5000

echo "=== one snapshot per round ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT
        operation,
        summary['added-records'],
        summary['changed-partition-count'],
        summary['clickhouse.refresh-cursor'] != ''
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TGT}'
    ORDER BY made_current_at
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${MV}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${SRC}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TGT}"
rm -rf "${TGT_PATH}"
