#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest row estimate subtracts the deleted rows it can count, matching the delete files to the
# remaining data files as the read does. The estimate stays imprecise.
# - T1: a position delete file scoped to one data file is subtracted.
# - T2: a position delete file covering several data files is not (Spark fixture, data_lake_corpus/iceberg/ice143_v2_position_deletes).
# - T2b: a DELETE touching two data files writes a delete file scoped to each, so both are subtracted.
# - T3: per partition: no filter, a filter that prunes the partition with the deletes, one that keeps only it.
# - T4: equality deletes are not subtracted, nor a removed delete entry (Spark fixture, see data_minio/deletes_db/README.md).
# - T5: a key whose range fits the rows but not the rows left after the deletes gets no counted NDV.
# - T6: a filter is estimated on the rows left after the deletes.
# - T7: deletes that leave 0 rows keep the estimate imprecise.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

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
cp -r "${CUR_DIR}/data_lake_corpus/iceberg/ice143_v2_position_deletes" "${LAKE}/ice143_v2_position_deletes"

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

# u: 2 data files (k 0-99, 100-199), the DELETE touches only the first, so its delete file is scoped to it.
# g: the same layout, the DELETE touches both files, so it writes a delete file for each.
# p: 1 data file per partition (us: odd k, eu: even k), the DELETE touches only the us file.
# del: 2 data files of 1000 rows, v has 5 values, the DELETE touches only the first, so its delete file is scoped to it.
# emptied: 1 data file of 50 rows, all deleted by a delete file scoped to it.
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
    CREATE TABLE del (k Int32, v Int32) ENGINE = IcebergLocal('${LAKE}/del') SETTINGS iceberg_format_version = 2;
    INSERT INTO del SELECT number, number % 5 FROM numbers(1000);
    INSERT INTO del SELECT number, number % 5 FROM numbers(1000, 1000);
    DELETE FROM del WHERE k < 500;
    CREATE TABLE emptied (k Int32, v Int64) ENGINE = IcebergLocal('${LAKE}/emptied') SETTINGS iceberg_format_version = 2;
    INSERT INTO emptied SELECT number, number FROM numbers(50);
    DELETE FROM emptied WHERE k < 50;
    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

echo '--- T1: delete file scoped to one data file'
labels u
echo '--- T2: 68 rows, 28 deleted by file-scoped position deletes, 3 by a delete file covering two data files'
labels "(SELECT id AS k FROM icebergLocal('${LAKE}/ice143_v2_position_deletes/'))"
echo '--- T2b: a DELETE touching two data files writes one delete file per data file, so both are subtracted'
labels g
echo '--- T3: no filter'
labels p
echo "--- T3: WHERE r = 'eu' prunes the partition with the deletes"
labels p "t.r = 'eu'"
echo "--- T3: WHERE r = 'us' keeps only that partition"
labels p "t.r = 'us'"
echo '--- T4: 1010 rows, a live file-scoped position delete of 30, a removed one of 20, equality deletes removing 99'
labels "(SELECT id AS k FROM icebergLocal('${LAKE}/eq_deletes_table/'))"
echo '--- T5: k is 0..199 and the deletes leave 180 rows, so the range 200 is not counted: 10% of the 200 rows'
${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query "EXPLAIN SELECT count() FROM mt AS m JOIN u AS t ON m.k = t.k" 2>&1 >/dev/null \
    | grep -oE 'Estimated statistics for [A-Za-z]+ t: .*' | head -n 1 | grep -oE 't: [0-9]+ rows|__table[0-9]+\.k: [0-9]+'
echo '--- T6: WHERE v = 1, 500 of 2000 rows deleted'
echo 'Expect ~300: the deletes leave 1500 rows, and v = 1 is 1 of its 5 values, so 1500 / 5.'
labels del "t.v = 1"
echo '--- T7: all 50 rows of a one-file table deleted'
echo 'Expect ~0: the subtraction leaves 0 rows, which stay an estimate.'
labels emptied

rm -rf "${LAKE}"
