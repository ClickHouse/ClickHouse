#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row count behind `use_iceberg_manifest_statistics` on broken metadata.
# T9: a negative `record_count` (the fixture of `04615`) gives unknown rows, not a huge number.
# T9b: a missing manifest file makes `EXPLAIN` of a join fail, as the read does (errors propagate);
# with the setting off `EXPLAIN` does not open the manifests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The corrupted metadata makes the server log expected warnings; keep them out of stderr.
# Exceptions still reach the client.
CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=fatal"}

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

# Runs a command and prints `no exception`, or the error code name it failed with.
outcome()
{
    local out
    if out=$("$@" 2>&1); then
        echo "no exception"
    else
        echo "${out}" | grep -o -E '\([A-Z_]{3,}\)' | head -1 | tr -d '()'
    fi
}

${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

cp -r "${CUR_DIR}/data_minio/iceberg_negative_record_count_test" "${LAKE}/negative"
echo '--- T9: negative record_count'
labels "SELECT count() FROM mt AS m JOIN icebergLocal('${LAKE}/negative') AS t ON m.k = t.order_number" ${ON}

# T9b: a table written here (a copied ClickHouse-written table keeps absolute manifest paths of the
# original), then its only manifest file is deleted. The metadata cache would hide the deletion.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE brk (k Int32, w Int64) ENGINE = IcebergLocal('${LAKE}/brk');
    INSERT INTO brk SELECT number, number FROM numbers(10);
"
MANIFESTS=$(find "${LAKE}/brk/metadata" -name '*.avro' ! -name 'snap-*')
echo "--- fixture brk: manifest files deleted"
echo "${MANIFESTS}" | grep -c '\.avro$'
rm -f ${MANIFESTS}

QUERY="SELECT count() FROM mt AS m JOIN brk AS s ON m.k = s.k"
echo '--- T9b: EXPLAIN of the join'
outcome labels "${QUERY}" --use_iceberg_metadata_files_cache=0 ${ON}
echo '--- T9b setting off: EXPLAIN of the join'
labels "${QUERY}" --use_iceberg_metadata_files_cache=0 ${OFF}

rm -rf "${LAKE}"
