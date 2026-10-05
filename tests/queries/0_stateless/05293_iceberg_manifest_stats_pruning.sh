#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count of an Iceberg read under a filter, as MergeTree without
# column statistics reports it. A filter that drops at least one live data file gives the rows of
# the remaining files, imprecise; a filter that drops nothing gives unknown; a filter that drops
# every file gives a precise 0. Delete files are not opened: rows before deletes, imprecise.
# T1: partition pruning by the manifest list, and the same query with the setting off. T2: min/max
# pruning. T3: partition pruning per data file (bucket transform). T4: every file pruned. T5: a filter
# that prunes nothing. T6: position deletes. T7: the walk at planning does not count its pruning in the
# read's counters. T8: it builds no `IN` set.

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
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1"
OFF="--use_iceberg_manifest_statistics=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
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
# b: one data file per bucket of k, each spanning k. Every p.v, b.v and d.k has a match in mt.
# The filters stay off the join keys: a filter on a join key is pushed to both sides.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE p (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO p SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE b (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/b') PARTITION BY (icebergBucket(4, k));
    INSERT INTO b SELECT number, number FROM numbers(1000);
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

# The remaining files hold only rows with r = 'us', so 15 is exact, but as for MergeTree a filter makes it imprecise.
T1="SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.r = 'us'"
echo "--- T1: partition pruning by the manifest list, WHERE p.r = 'us'"
labels "${T1}" ${ON}
echo '--- T1: setting off'
labels "${T1}" ${OFF}
echo '--- T1: rows of the join'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "${T1}"

T2="SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.k >= 100"
echo '--- T2: min/max pruning, WHERE p.k >= 100'
labels "${T2}" ${ON}
echo '--- T2: rows of the join'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "${T2}"

# Min/max keeps every bucket file and the manifest-list pruner cannot use a bucket transform, so only the
# per-file pruner drops 3 files. The estimate is the 229 rows of the remaining file, of which one matches.
T3="SELECT count() FROM mt AS m JOIN b ON m.x = b.v WHERE b.k = 500"
echo '--- T3: partition pruning per data file, WHERE b.k = 500'
labels "${T3}" ${ON}
echo '--- T3: rows of the join'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "${T3}"

echo '--- T4: every file pruned, WHERE p.k >= 1000000'
labels "SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE p.k >= 1000000" ${ON}

echo '--- T5: a filter that prunes nothing, WHERE t.x = 5'
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x = 5" ${ON}

# Deleted rows are not subtracted, so the estimate overreports (100 rows, 80 live).
T6="SELECT count() FROM mt AS m JOIN d ON m.k = d.k"
echo '--- T6: position deletes, 100 rows, 20 deleted'
labels "${T6}" ${ON}
echo '--- T6: rows of the join'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "${T6}"

# No double counting: the walk at planning and the read both prune the same 3 files, only the read counts them.
echo '--- T7: files pruned by min/max when T2 runs'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query_id="${CLICKHOUSE_DATABASE}_t7" --query "${T2} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND query_id = '${CLICKHOUSE_DATABASE}_t7'"

# Building the set would run `throwIf`; an unusable filter prunes nothing, so the rows are unknown.
echo '--- T8: GLOBAL IN with throwIf, EXPLAIN only'
labels "SELECT count() FROM mt AS m JOIN unp AS t ON m.k = t.k WHERE t.x GLOBAL IN (SELECT throwIf(number = 0) FROM numbers(1))" ${ON}

rm -rf "${LAKE}"
