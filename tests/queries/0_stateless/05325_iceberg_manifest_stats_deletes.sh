#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row estimate subtracts the deleted rows it can count, matching the delete files to the
# remaining data files as the read does. The estimate stays imprecise.
# - T1: a position delete file scoped to one data file is subtracted.
# - T2: a position delete file covering several data files is not; `IcebergRowEstimateSkippedPositionDeleteFiles` counts it.
# - T3: per partition: no filter, a filter that prunes the partition with the deletes, one that keeps only it.
# - T4: equality deletes are not subtracted, nor a removed delete entry (Spark fixture, see data_minio/deletes_db/README.md).

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
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/deletes_db/eq_deletes_table" "${LAKE}/eq_deletes_table"

# Prints the `Join:` line of the logical plan, then the rows the read returns. Usage: labels <table> [<filter>].
labels()
{
    local table="$1"
    local filter="${2:-1}"
    ${CLICKHOUSE_CLIENT} ${PINS} --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT count() FROM mt AS m JOIN ${table} AS t ON m.k = t.k WHERE ${filter})
        WHERE explain LIKE '%Join: %'"
    echo "live rows: $(${CLICKHOUSE_CLIENT} ${PINS} --query "SELECT count() FROM ${table} AS t WHERE ${filter}")"
}

# Prints how many position delete files the estimate skipped for the join with <table>. Usage: skipped <tag> <table>.
skipped()
{
    local query_id="${CLICKHOUSE_DATABASE}_$1"
    ${CLICKHOUSE_CLIENT} ${PINS} --query_id="${query_id}" --query "
        EXPLAIN keep_logical_steps = 1 SELECT count() FROM mt AS m JOIN $2 AS t ON m.k = t.k" > /dev/null
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    echo "skipped delete files: $(${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['IcebergRowEstimateSkippedPositionDeleteFiles'] FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND query_id = '${query_id}'")"
}

# u: 2 data files (k 0-99, 100-199), the DELETE touches only the first, so its delete file is scoped to it.
# g: the same layout, the DELETE touches both files, so its delete file covers both.
# p: 1 data file per partition (us: odd k, eu: even k), the DELETE touches only the us file.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE u (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/u') SETTINGS iceberg_format_version = 2;
    INSERT INTO u SELECT number, number FROM numbers(100);
    INSERT INTO u SELECT number, number FROM numbers(100, 100);
    DELETE FROM u WHERE k < 20;
    CREATE TABLE g (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/g') SETTINGS iceberg_format_version = 2;
    INSERT INTO g SELECT number, number FROM numbers(100);
    INSERT INTO g SELECT number, number FROM numbers(100, 100);
    DELETE FROM g WHERE k % 10 = 0;
    CREATE TABLE p (k Int32, r String, v Int64) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r) SETTINGS iceberg_format_version = 2;
    INSERT INTO p SELECT number, ['eu', 'us'][number % 2 + 1], number FROM numbers(200);
    DELETE FROM p WHERE r = 'us' AND k < 40;
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

echo '--- T1: delete file scoped to one data file'
labels u
skipped t1 u
echo '--- T2: delete file covering two data files'
labels g
skipped t2 g
echo '--- T3: no filter'
labels p
echo "--- T3: WHERE r = 'eu' prunes the partition with the deletes"
labels p "t.r = 'eu'"
echo "--- T3: WHERE r = 'us' keeps only that partition"
labels p "t.r = 'us'"
echo '--- T4: 1010 rows, a live file-scoped position delete of 30, a removed one of 20, equality deletes removing 99'
labels "(SELECT id AS k FROM icebergLocal('${LAKE}/eq_deletes_table/'))"

rm -rf "${LAKE}"
