#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count behind `use_iceberg_manifest_statistics` below an aggregation
# and under the `make_distributed_plan` fallback.
# T10b: an aggregation over an Iceberg read inside a join. With the NDV of the key from the manifests
# (`use_iceberg_manifest_column_statistics = 1`) the aggregation estimates its groups, exact, so no hint line names it. A filter
# that prunes nothing leaves the read without rows and column statistics: the aggregation is imprecise,
# and the debug log lists it in the data lake hint line, not in the MergeTree `Consider creating column
# statistics` line, which still lists the MergeTree relation without statistics (the positive control of
# the log capture).
# T10c: `make_distributed_plan = 1` falls back to local execution on `ReadFromObjectStorage`; the
# labels are those of the local plan, and the query runs without an exception.
# Every arm also runs with the gate off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_DEBUG=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=debug"}

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1 --use_iceberg_manifest_column_statistics=1"
OFF="--use_iceberg_manifest_statistics=0"
DISTRIBUTED="--make_distributed_plan=1 --distributed_plan_fallback_to_local_execution=1"

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

# mt_no_stats has no column statistics, so the MergeTree hint line always names it.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE ice_big (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/ice_big');
    INSERT INTO ice_big SELECT number % 1000, number FROM numbers(100000);
    CREATE TABLE ice_small (k Int32, w Int64) ENGINE = IcebergLocal('${LAKE}/ice_small');
    INSERT INTO ice_small SELECT number, number FROM numbers(10);
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
    CREATE TABLE mt_no_stats (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO mt_no_stats SELECT number, number FROM numbers(1000);
    CREATE TABLE twin_big (k Int32, v Int64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_big SELECT number % 1000, number FROM numbers(100000);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"

T10B="SELECT count() FROM mt_no_stats AS m JOIN (SELECT k, count() AS c FROM ice_big GROUP BY k) AS ice_agg ON m.k = ice_agg.k"
T10B_FILTERED="SELECT count() FROM mt_no_stats AS m
    JOIN (SELECT k, count() AS c FROM ice_big WHERE v % 2 = 0 GROUP BY k) AS ice_filtered ON m.k = ice_filtered.k"

# Prints the relations of the MergeTree hint line and counts the data lake hint lines naming a relation.
# Usage: hint_lines <query> <relation> [client flags].
hint_lines()
{
    local query="$1"
    local relation="$2"
    shift 2
    local log
    log=$(${CLICKHOUSE_CLIENT_DEBUG} ${PINS} "$@" --query "EXPLAIN keep_logical_steps = 1, actions = 1 ${query}" 2>&1 >/dev/null)
    echo "relations in the column statistics hint: $(echo "${log}" | grep 'Consider creating column statistics' \
        | sed -n 's/.*for join reordering: \(.*\)\. The chosen join order.*/\1/p')"
    echo "data lake hint lines naming ${relation}: $(echo "${log}" | grep 'derived from data lake metadata' | grep -c "${relation}")"
}

echo '--- T10b twin: aggregation over a MergeTree table with a cardinality and NDV hint'
labels "SELECT count() FROM mt_no_stats AS m JOIN (SELECT k, count() AS c FROM twin_big GROUP BY k) AS ice_agg ON m.k = ice_agg.k" \
    --param__internal_join_table_stat_hints='{"twin_big": {"cardinality": 100000, "distinct_keys": {"k": 1000}}}'
echo '--- T10b gate on: labels'
labels "${T10B}" ${ON}
echo '--- T10b gate on: debug log'
hint_lines "${T10B}" ice_agg ${ON}
echo '--- T10b gate on, a filter that prunes nothing: labels'
labels "${T10B_FILTERED}" ${ON}
echo '--- T10b gate on, a filter that prunes nothing: debug log'
hint_lines "${T10B_FILTERED}" ice_filtered ${ON}
echo '--- T10b gate off: labels'
labels "${T10B}" ${OFF}
echo '--- T10b gate off: debug log'
hint_lines "${T10B}" ice_agg ${OFF}

T10C="SELECT count() FROM ice_big AS b JOIN mt AS m ON b.k = m.k JOIN ice_small AS s ON m.k = s.k"

# Prints whether the plan fell back to local execution.
fell_back()
{
    if ${CLICKHOUSE_CLIENT_DEBUG} ${PINS} ${DISTRIBUTED} "$@" --query "EXPLAIN ${T10C}" 2>&1 >/dev/null \
        | grep -q 'falling back to local execution'; then
        echo "fell back to local execution: yes"
    else
        echo "fell back to local execution: no"
    fi
}

echo '--- T10c gate on: make_distributed_plan with the fallback'
fell_back ${ON}
labels "${T10C}" ${DISTRIBUTED} ${ON}
${CLICKHOUSE_CLIENT} ${PINS} ${DISTRIBUTED} ${ON} --query "${T10C}"
echo '--- T10c gate off'
fell_back ${OFF}
labels "${T10C}" ${DISTRIBUTED} ${OFF}
${CLICKHOUSE_CLIENT} ${PINS} ${DISTRIBUTED} ${OFF} --query "${T10C}"

rm -rf "${LAKE}"
