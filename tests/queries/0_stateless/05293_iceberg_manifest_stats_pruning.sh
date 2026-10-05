#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count of an Iceberg read under a filter, as MergeTree without
# column statistics reports it. A filter that drops at least one live data file gives the rows of
# the remaining files, imprecise; a filter that drops nothing gives unknown; a filter that drops
# every file gives a precise 0. Delete files are not opened: rows before deletes, imprecise.
# T3 partition (manifest list, and data files with the manifest-list pruning off) and min/max pruning,
# T3b every file pruned, T3c a row policy, T3d partition pruning off, T4 position deletes.
# T5: the walk at planning does not bump the read's pruning counters. T6: it builds no `IN` set.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1 --use_iceberg_manifest_statistics=1"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
TEST_USER="${CLICKHOUSE_DATABASE}_user"
TEST_POLICY="${CLICKHOUSE_DATABASE}_policy"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Prints the `Join:` and `ResultRows:` lines of the logical plan. Usage: labels <query> [client flags].
labels()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${query})
        WHERE explain LIKE '%Join: %' OR explain LIKE '%ResultRows: %'"
}

# p: 5 data files, each INSERT writes one manifest per value of r. us: 10 + 5 rows, eu: 10 + 5, asia: 10.
# k is in [0, 29] in the first INSERT's files and in [100, 109] in the second's.
# The filters stay off the join keys: a filter on a join key is pushed to both sides.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE p (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO p SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE unp (k Int32, x Int32) ENGINE = IcebergLocal('${LAKE}/unp');
    INSERT INTO unp SELECT number, number % 1000 + 1 FROM numbers(10000);
    CREATE TABLE d (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/d') SETTINGS iceberg_format_version = 2;
    INSERT INTO d SELECT number, number FROM numbers(100);
    DELETE FROM d WHERE k < 20;
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

echo '--- fixture: files and rows per Iceberg table and content'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, content, count(), sum(record_count), arraySort(groupArray(record_count))
    FROM system.iceberg_files WHERE database = currentDatabase()
    GROUP BY table, content ORDER BY table, content"

for CASE in "partition|p.r = 'us'" "minmax|p.k >= 100"; do
    NAME="${CASE%%|*}"
    QUERY="SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE ${CASE#*|}"
    echo "--- T3 ${NAME}: WHERE ${CASE#*|}"
    labels "${QUERY}" --query_id="${CLICKHOUSE_DATABASE}_t5_${NAME}_explain"
    ${CLICKHOUSE_CLIENT} ${PINS} --query_id="${CLICKHOUSE_DATABASE}_t5_${NAME}_executed" --query "${QUERY}"
done

echo "--- T3 partition, manifest-list pruning off: WHERE p.r = 'us'"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.r = 'us'" --use_iceberg_manifest_list_partition_pruning=0
echo "--- T3b: WHERE p.k >= 1000000"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.k >= 1000000"
echo "--- T3 unpartitioned: WHERE t.x = 5 drops no file"
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x = 5"
echo "--- T3d: use_iceberg_partition_pruning = 0, WHERE p.r = 'us'"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.r = 'us'" --use_iceberg_partition_pruning=0

echo '--- T4: 100 rows, 20 deleted by a position delete file'
labels "SELECT count() FROM mt AS m JOIN d AS b ON m.k = b.k"

# The read counts its own pruning once; `EXPLAIN` runs only the walk.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo '--- T5: PartitionPrunedFiles, PartitionPrunedManifestFiles, MinMaxIndexPrunedFiles, TrivialCountOptimizationApplied'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        replaceOne(query_id, currentDatabase() || '_t5_', ''),
        ProfileEvents['IcebergPartitionPrunedFiles'],
        ProfileEvents['IcebergPartitionPrunedManifestFiles'],
        ProfileEvents['IcebergMinMaxIndexPrunedFiles'],
        ProfileEvents['IcebergTrivialCountOptimizationApplied']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND startsWith(query_id, currentDatabase() || '_t5_')
    ORDER BY query_id"

# Building the set would run `throwIf`; an unusable filter prunes nothing, so the rows are unknown.
echo '--- T6: GLOBAL IN with throwIf, EXPLAIN only'
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x GLOBAL IN (SELECT throwIf(number = 0) FROM numbers(1))"

# T3c: the policy k >= 100 drops the first INSERT's 3 files by min/max, as the executed read does.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${TEST_USER} IDENTIFIED WITH plaintext_password BY 'policy_pwd'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO ${TEST_USER}"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.p FOR SELECT USING k >= 100 TO ${TEST_USER}"
echo "--- T3c: row policy k >= 100, no WHERE"
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v" --user "${TEST_USER}" --password policy_pwd
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${TEST_POLICY} ON ${CLICKHOUSE_DATABASE}.p"
${CLICKHOUSE_CLIENT} --query "DROP USER ${TEST_USER}"

rm -rf "${LAKE}"
