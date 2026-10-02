#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the planning-time manifest walk behind `use_iceberg_manifest_statistics` must not
# have the read's side effects.
# T5: the Iceberg pruning and trivial-count ProfileEvents of a join are the same with the gate off
# and on, for `EXPLAIN` (0 everywhere) and for the executed query (the read's own pruning, not
# doubled). The labels show that the walk ran.
# T6: a `GLOBAL IN` set is not built at planning time: the walk treats the filter as unusable, so
# the relation reports unknown and no exception is thrown by `throwIf`.

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

# p: 5 data files, per INSERT one per value of r (us: 10 + 5 rows); k is in [100, 109] only in the
# second INSERT's 2 files. The filters stay off the join keys: a filter on a join key is pushed to both sides.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE p (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, ['us', 'eu', 'asia'][number % 3 + 1], number FROM numbers(30);
    INSERT INTO p SELECT number, ['us', 'eu'][number % 2 + 1], number FROM numbers(100, 10);
    CREATE TABLE ice_a (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/ice_a');
    INSERT INTO ice_a SELECT number, number FROM numbers(100);
    CREATE TABLE ice_s (k Int32, w Int64) ENGINE = IcebergLocal('${LAKE}/ice_s');
    INSERT INTO ice_s SELECT number, number FROM numbers(10);
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

for CASE in "partition|p.r = 'us'" "minmax|p.k >= 100"; do
    NAME="${CASE%%|*}"
    FILTER="${CASE#*|}"
    QUERY="SELECT count() FROM mt AS m JOIN p ON m.x = p.v WHERE ${FILTER}"
    for GATE in 0 1; do
        if [ "${GATE}" = 1 ]; then GATE_FLAG="${ON}"; else GATE_FLAG="${OFF}"; fi
        echo "--- T5 ${NAME}: EXPLAIN, gate ${GATE}"
        labels "${QUERY}" ${GATE_FLAG} --query_id="${CLICKHOUSE_DATABASE}_t5_${NAME}_explain_gate${GATE}"
        echo "--- T5 ${NAME}: executed, gate ${GATE}"
        ${CLICKHOUSE_CLIENT} ${PINS} ${GATE_FLAG} --query_id="${CLICKHOUSE_DATABASE}_t5_${NAME}_executed_gate${GATE}" \
            --query "${QUERY}"
    done
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo '--- T5 counters: PartitionPrunedFiles, PartitionPrunedManifestFiles, MinMaxIndexPrunedFiles, TrivialCountOptimizationApplied'
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

T6="SELECT count() FROM ice_s AS s JOIN ice_a AS a ON s.k = a.k
    WHERE a.v GLOBAL IN (SELECT throwIf(number = 0) FROM numbers(1))"
echo '--- T6 gate on: GLOBAL IN with throwIf, EXPLAIN only'
labels "${T6}" ${ON}
echo '--- T6 gate off'
labels "${T6}" ${OFF}

rm -rf "${LAKE}"
