#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count behind `use_iceberg_manifest_statistics` comes from the
# snapshot the read uses. T7: two INSERTs (10, then 5 rows); time travel to the first snapshot by id
# and by timestamp reports 10, the latest snapshot 15; a `RENAME COLUMN` (a new schema, no new
# snapshot) changes neither. Every arm also runs with the gate off.

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

${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
    CREATE TABLE tt (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/tt');
    INSERT INTO tt SELECT number, number FROM numbers(10);
"
# The only snapshot so far is the first one.
FIRST_ID=$(${CLICKHOUSE_CLIENT} --query "
    SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'tt'")
FIRST_MS=$(${CLICKHOUSE_CLIENT} --query "
    SELECT toUnixTimestamp64Milli(made_current_at) FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'tt'")
${CLICKHOUSE_CLIENT} ${PINS} --query "INSERT INTO tt SELECT number, number FROM numbers(100, 5)"

QUERY="SELECT count() FROM mt AS m JOIN tt AS b ON m.k = b.k"

check_snapshots()
{
    echo "--- fixture: snapshots; rows in the first and in the latest snapshot"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = 'tt'"
    ${CLICKHOUSE_CLIENT} --iceberg_snapshot_id="${FIRST_ID}" --query "SELECT count() FROM tt"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM tt"

    echo "--- T7 $1 gate on: first snapshot by iceberg_snapshot_id"
    labels "${QUERY}" --iceberg_snapshot_id="${FIRST_ID}" ${ON}
    echo "--- T7 $1 gate on: first snapshot by iceberg_timestamp_ms"
    labels "${QUERY}" --iceberg_timestamp_ms="${FIRST_MS}" ${ON}
    echo "--- T7 $1 gate on: latest snapshot"
    labels "${QUERY}" ${ON}
    echo "--- T7 $1 gate off: first snapshot by iceberg_snapshot_id"
    labels "${QUERY}" --iceberg_snapshot_id="${FIRST_ID}" ${OFF}
    echo "--- T7 $1 gate off: latest snapshot"
    labels "${QUERY}" ${OFF}
}

check_snapshots "before RENAME COLUMN,"

${CLICKHOUSE_CLIENT} ${PINS} --query "ALTER TABLE tt RENAME COLUMN v TO v2"

check_snapshots "after RENAME COLUMN,"

rm -rf "${LAKE}"
