#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_column_statistics`, the filter of an Iceberg read is estimated on the column
# statistics of its remaining data files, as for MergeTree tables with column statistics.
# - T1: filters that prune no file. Equality and `IN` on an integer divide by its counted NDV, `IS NULL` uses the NULL
#   count, a range interpolates the bounds, a string equality gets the default factor. Without column statistics, no estimate.
# - T2: filters that prune files: the selectivity applies to the remaining files.
# - T3: a filter on an identity-partitioned string keeps the rows of its partition.
# - T4: a file-scoped delete scales the filtered estimate by the share of live rows.
# - T5: a filter on a 5-row table, which also reaches the table joined on its key, no longer hides the rows of the joins
#   above it: the plan and the rows put into hash tables, with column statistics off and on.

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
ON="--use_iceberg_manifest_column_statistics=1"
OFF="--use_iceberg_manifest_column_statistics=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Prints the `Join:` line of the plan of a join of <table> with `dim`, then the rows the read returns.
# Usage: labels <table> <key> <filter> [client flags].
labels()
{
    local table="$1" key="$2" filter="$3"
    shift 3
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
            SELECT count() FROM ${table} AS t JOIN dim ON t.${key} = dim.v WHERE ${filter})
        WHERE explain LIKE '%Join: %'"
    echo "rows: $(${CLICKHOUSE_CLIENT} ${PINS} --query "SELECT count() FROM ${table} AS t WHERE ${filter}")"
}

# t: 4 data files of 2500 rows with disjoint k and d ranges; v has 5 values, n is 90% NULL, s is 'rare' in 2% of rows.
# p: identity-partitioned by a string, one data file per partition (us: 3000 rows, eu: 7000).
# del: 2 data files of 1000 rows; the DELETE touches only the first, so its delete file is scoped to it.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE t (k Int64, v Int32, s String, n Nullable(Int32), d Date) ENGINE = IcebergLocal('${LAKE}/t');
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(0, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(2500, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(5000, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(7500, 2500);
    CREATE TABLE p (k Int64, r String) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, if(number % 10 < 3, 'us', 'eu') FROM numbers(10000);
    CREATE TABLE del (k Int64, v Int32) ENGINE = IcebergLocal('${LAKE}/del') SETTINGS iceberg_format_version = 2;
    INSERT INTO del SELECT number, number % 5 FROM numbers(1000);
    INSERT INTO del SELECT number, number % 5 FROM numbers(1000, 1000);
    DELETE FROM del WHERE k < 500;
    CREATE TABLE dim (v Int32) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim SELECT number FROM numbers(5);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, content, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() GROUP BY table, content ORDER BY table, content"

echo '--- T1: v = 3, 10000 / 5 values'
labels t v 't.v = 3' ${ON}
echo '--- T1: v = 3, without column statistics'
labels t v 't.v = 3' ${OFF}
echo '--- T1: v IN (1, 2)'
labels t v 't.v IN (1, 2)' ${ON}
echo '--- T1: n IS NULL'
labels t v 't.n IS NULL' ${ON}
echo '--- T1: k BETWEEN 1000 AND 8999, every file overlaps'
labels t v 't.k BETWEEN 1000 AND 8999' ${ON}
echo "--- T1: s = 'rare', strings have no bounds in the statistics and no counted NDV, so 1%"
labels t v "t.s = 'rare'" ${ON}

echo '--- T2: k < 1000, 1 of 4 files remains'
labels t v 't.k < 1000' ${ON}
echo "--- T2: d = '2024-01-05', 1 of 4 files remains, 25 days in it"
labels t v "t.d = '2024-01-05'" ${ON}

echo "--- T3: r = 'us', the partition of 3000 rows"
labels p k "t.r = 'us'" ${ON}

echo '--- T4: v = 1 with 500 of 2000 rows deleted, 2000 / 5 * 1500 / 2000'
labels del v 't.v = 1' ${ON}

# T5 tables: l -> o -> c -> g, every key dense; g is filtered.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE l (lo Int64) ENGINE = IcebergLocal('${LAKE}/l');
    INSERT INTO l SELECT number % 10000 + 1 FROM numbers(30000);
    CREATE TABLE o (id Int64, oc Int64) ENGINE = IcebergLocal('${LAKE}/o');
    INSERT INTO o SELECT number + 1, number % 1000 + 1 FROM numbers(10000);
    CREATE TABLE c (id Int64, cg Int32) ENGINE = IcebergLocal('${LAKE}/c');
    INSERT INTO c SELECT number + 1, number % 5 FROM numbers(1000);
    CREATE TABLE g (id Int32) ENGINE = IcebergLocal('${LAKE}/g');
    INSERT INTO g SELECT number FROM numbers(5);
"
T5="SELECT count() FROM l JOIN o ON l.lo = o.id JOIN c ON o.oc = c.id JOIN g ON c.cg = g.id WHERE g.id = 2"
echo '--- T5: plan, column statistics off: the filtered reads are unknown, so are the joins above them'
${CLICKHOUSE_CLIENT} ${PINS} ${OFF} --query "
    SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${T5}) WHERE explain LIKE '%Join: %'"
echo '--- T5: plan, column statistics on'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "
    SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${T5}) WHERE explain LIKE '%Join: %'"
echo '--- T5: result, then rows put into hash tables, column statistics off and on'
${CLICKHOUSE_CLIENT} ${PINS} ${OFF} --query_id="${CLICKHOUSE_DATABASE}_t5_off" --query "${T5}"
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query_id="${CLICKHOUSE_DATABASE}_t5_on" --query "${T5}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, currentDatabase() || '_t5_', ''), ProfileEvents['JoinBuildTableRowCount']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND startsWith(query_id, currentDatabase() || '_t5_')
    ORDER BY query_id"

rm -rf "${LAKE}"
