#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_statistics`, join reordering gets Iceberg row counts from the manifests.
# - T1: order of a 3-way join with the setting off and on, and the rows its hash tables get.
# - T2: the smaller table becomes the build side.
# - T3: a table that was never written has 0 rows.
# - T4: the count comes from the snapshot the read uses.
# - T5: an aggregation over an Iceberg read keeps an estimate and is named in the data lake hint line.

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

# Runs a query with the setting off and on, then prints for each run the rows put into hash tables, which are
# the build sides (`JoinBuildTableRowCount`), and the rows probed (`JoinProbeTableRowCount`).
# Usage: join_rows <tag> <query>.
join_rows()
{
    local tag="$1"
    local query="$2"
    ${CLICKHOUSE_CLIENT} ${PINS} ${OFF} --query_id="${CLICKHOUSE_DATABASE}_${tag}_setting_off" --query "${query} FORMAT Null"
    ${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query_id="${CLICKHOUSE_DATABASE}_${tag}_setting_on" --query "${query} FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT replaceOne(query_id, currentDatabase() || '_${tag}_', ''),
            ProfileEvents['JoinBuildTableRowCount'], ProfileEvents['JoinProbeTableRowCount']
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND startsWith(query_id, currentDatabase() || '_${tag}_')
        ORDER BY query_id"
}

# `uniq` gives `mt` an exact count and an NDV equal to its rows, so `ResultRows` is plain arithmetic.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE ice_big (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/ice_big');
    INSERT INTO ice_big SELECT number % 1000, number FROM numbers(100000);
    CREATE TABLE ice_small (k Int32, w Int64) ENGINE = IcebergLocal('${LAKE}/ice_small');
    INSERT INTO ice_small SELECT number, number FROM numbers(10);
    CREATE TABLE ice_empty (k Int32) ENGINE = IcebergLocal('${LAKE}/ice_empty');
    CREATE TABLE tt (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/tt');
    INSERT INTO tt SELECT number, number FROM numbers(10);
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"
# The only snapshot of tt so far is the first one.
FIRST_ID=$(${CLICKHOUSE_CLIENT} --query "
    SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'tt'")
${CLICKHOUSE_CLIENT} ${PINS} --query "INSERT INTO tt SELECT number, number FROM numbers(100, 5)"

echo '--- fixture: data files and rows per Iceberg table, snapshots of ice_empty and tt'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(table = 'ice_empty'), countIf(table = 'tt') FROM system.iceberg_history WHERE database = currentDatabase()"

T1="SELECT count() FROM ice_big AS b JOIN mt AS m ON b.k = m.k JOIN ice_small AS s ON m.k = s.k"
echo '--- T1: 3-way join'
labels "${T1}" ${ON}
echo '--- T1: 3-way join, setting off'
labels "${T1}" ${OFF}
echo '--- T1: rows into the hash tables and rows probed, setting off and on'
join_rows t1 "${T1}"

echo '--- T2: rows of the build side and of the probe side, setting off and on'
join_rows t2 "SELECT m.k FROM mt AS m JOIN ice_big AS b ON m.k = b.k"

echo '--- T3: a table with no snapshot'
labels "SELECT count() FROM mt AS m JOIN ice_empty AS t ON m.k = t.k" ${ON}

T4="SELECT count() FROM mt AS m JOIN tt AS b ON m.k = b.k"
echo '--- T4: first snapshot by iceberg_snapshot_id'
labels "${T4}" --iceberg_snapshot_id="${FIRST_ID}" ${ON}
echo '--- T4: latest snapshot'
labels "${T4}" ${ON}

# Without the NDV of `k` the aggregation keeps the input rows, so it overreports (100000 groups for 1000 keys);
# the point is that the relation still gets an estimate, and the data lake hint line names it.
T5="SELECT count() FROM mt AS m JOIN (SELECT k, count() AS c FROM ice_big GROUP BY k) AS ice_agg ON m.k = ice_agg.k"
echo '--- T5: aggregation over an Iceberg read'
labels "${T5}" ${ON}
LOG=$(${CLICKHOUSE_CLIENT_DEBUG} ${PINS} ${ON} --query "EXPLAIN keep_logical_steps = 1, actions = 1 ${T5}" 2>&1 >/dev/null)
echo "data lake hint lines naming ice_agg: $(echo "${LOG}" | grep 'derived from data lake metadata' | grep -c 'ice_agg')"
echo "column statistics hint lines naming ice_agg: $(echo "${LOG}" | grep 'Consider creating column statistics' | grep -c 'ice_agg')"

rm -rf "${LAKE}"
