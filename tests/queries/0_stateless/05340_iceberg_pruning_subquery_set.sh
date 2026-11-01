#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Partition pruning and min/max pruning of Iceberg manifest files use the sets of `IN (subquery)`,
# with any number of manifest decoding threads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_in_subquery"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# One INSERT per partition, so every data file has its own manifest and the decoding threads share
# the filter. Background compaction would merge them, so it is off.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, v Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (p)
    SETTINGS allow_experimental_iceberg_compaction = 0;
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (1, 100), (1, 101);
    INSERT INTO ${TABLE} VALUES (2, 200), (2, 201);
    INSERT INTO ${TABLE} VALUES (3, 300), (3, 301);
    INSERT INTO ${TABLE} VALUES (4, 400), (4, 401);
" < /dev/null

# Manifest-list pruning reports the files it skips through the same counters and would answer before
# the manifest files are read, so it is off for every probe.
probe() { # probe <tag> <query> <settings>
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --use_iceberg_manifest_list_partition_pruning=0 --query "
        $2 SETTINGS $3, log_comment = '${CLICKHOUSE_DATABASE}_$1'"
}

for concurrency in 1 4; do
    S="iceberg_manifest_decode_concurrency = ${concurrency}"
    probe "p_sub_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE p IN (SELECT 2)" "$S"
    probe "v_sub_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE v IN (SELECT 301)" "$S"
    probe "p_lit_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE p IN (2)" "$S"
    probe "v_lit_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE v IN (301)" "$S"
    # Without index use for subquery sets nothing is pruned: the counters observe the subquery sets.
    probe "p_sub_off_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE p IN (SELECT 2)" "$S, use_index_for_in_with_subqueries = 0"
    probe "v_sub_off_${concurrency}" "SELECT sum(v) FROM ${TABLE} WHERE v IN (SELECT 301)" "$S, use_index_for_in_with_subqueries = 0"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''),
        max(ProfileEvents['IcebergPartitionPrunedFiles']) > 0,
        max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '${CLICKHOUSE_DATABASE}_')
    GROUP BY log_comment
    ORDER BY log_comment
    SETTINGS enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
