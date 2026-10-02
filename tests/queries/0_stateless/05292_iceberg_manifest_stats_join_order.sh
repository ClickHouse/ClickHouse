#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_statistics = 1` an Iceberg read reports the row count
# summed from its manifest files to join reordering, labelled like an exact MergeTree count.
# T1: join order of a 3-way join, the same for both text orders. T2: the smaller table becomes the
# build side. T8: a table that was never written reports 0 rows. T10a: the `icebergLocal` table
# function gives the same labels. Every arm also runs with the gate off.
# The MergeTree twins with cardinality-only hints predict the numbers; only their labels differ.

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

# Prints the reads of the physical plan in order; the second input of the join is its build side.
reads()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT replaceOne(trimLeft(explain), currentDatabase() || '.', '') FROM (EXPLAIN actions = 1 ${query})
        WHERE explain LIKE '%ReadFrom%'"
}

# `uniq` gives `mt` an exact count and an NDV equal to its rows, so `ResultRows` is plain arithmetic.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE ice_big (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/ice_big');
    INSERT INTO ice_big SELECT number % 1000, number FROM numbers(100000);
    CREATE TABLE ice_small (k Int32, w Int64) ENGINE = IcebergLocal('${LAKE}/ice_small');
    INSERT INTO ice_small SELECT number, number FROM numbers(10);
    CREATE TABLE ice_empty (k Int32) ENGINE = IcebergLocal('${LAKE}/ice_empty');
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
    CREATE TABLE twin_big (k Int32, v Int64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_big SELECT number % 1000, number FROM numbers(100000);
    CREATE TABLE twin_small (k Int32, w Int64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS index_granularity = 8192, auto_statistics_types = '';
    INSERT INTO twin_small SELECT number, number FROM numbers(10);
"

echo '--- fixture: data files and rows per Iceberg table, snapshots of ice_empty'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table ORDER BY table"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'ice_empty'"

HINTS='{"twin_big": {"cardinality": 100000}, "twin_small": {"cardinality": 10}}'
T1_BMS="SELECT count() FROM ice_big AS b JOIN mt AS m ON b.k = m.k JOIN ice_small AS s ON m.k = s.k"
T1_SMB="SELECT count() FROM ice_small AS s JOIN mt AS m ON s.k = m.k JOIN ice_big AS b ON m.k = b.k"

echo '--- T1 twin: text order b, m, s'
labels "SELECT count() FROM twin_big AS b JOIN mt AS m ON b.k = m.k JOIN twin_small AS s ON m.k = s.k" \
    --param__internal_join_table_stat_hints="${HINTS}"
echo '--- T1 twin: text order s, m, b'
labels "SELECT count() FROM twin_small AS s JOIN mt AS m ON s.k = m.k JOIN twin_big AS b ON m.k = b.k" \
    --param__internal_join_table_stat_hints="${HINTS}"
echo '--- T1 gate on: text order b, m, s'
labels "${T1_BMS}" ${ON}
echo '--- T1 gate on: text order s, m, b'
labels "${T1_SMB}" ${ON}
echo '--- T1 gate off: text order b, m, s'
labels "${T1_BMS}" ${OFF}
echo '--- T1 gate off: text order s, m, b'
labels "${T1_SMB}" ${OFF}

echo '--- T2 twin: reads in order, the second one is the build side'
reads "SELECT m.k FROM mt AS m JOIN twin_big AS b ON m.k = b.k" --param__internal_join_table_stat_hints="${HINTS}"
echo '--- T2 gate on'
reads "SELECT m.k FROM mt AS m JOIN ice_big AS b ON m.k = b.k" ${ON}
echo '--- T2 gate off'
reads "SELECT m.k FROM mt AS m JOIN ice_big AS b ON m.k = b.k" ${OFF}

# An empty MergeTree table prints a bare label (its read is replaced), so the twin uses a hint of 0.
echo '--- T8 twin: cardinality 0'
labels "SELECT count() FROM mt AS m JOIN twin_small AS t ON m.k = t.k" \
    --param__internal_join_table_stat_hints='{"twin_small": {"cardinality": 0}}'
echo '--- T8 gate on: a table with no snapshot'
labels "SELECT count() FROM mt AS m JOIN ice_empty AS t ON m.k = t.k" ${ON}
echo '--- T8 gate off'
labels "SELECT count() FROM mt AS m JOIN ice_empty AS t ON m.k = t.k" ${OFF}

T10A="SELECT count() FROM icebergLocal('${LAKE}/ice_big') AS b JOIN mt AS m ON b.k = m.k
    JOIN icebergLocal('${LAKE}/ice_small') AS s ON m.k = s.k"
echo '--- T10a gate on: table function'
labels "${T10A}" ${ON}
echo '--- T10a gate off'
labels "${T10A}" ${OFF}

rm -rf "${LAKE}"
