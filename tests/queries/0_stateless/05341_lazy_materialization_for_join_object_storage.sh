#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Minio

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization over joins with an object storage read: the columns of a Parquet file on S3 that only the
# result needs are read for the rows the `LIMIT` returns, addressed by the row index the read registers, as for a
# MergeTree table. The queries run with lazy materialization over joins enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_join"
AUTH="'test', 'testtest'"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${URL}/data_1.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s
    FROM numbers(0, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION s3('${URL}/data_2.parquet', ${AUTH}, 'Parquet')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s
    FROM numbers(1000, 1000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    DROP TABLE IF EXISTS mt;
    CREATE TABLE mt (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
    INSERT INTO mt SELECT number * 3, concat('mt_', toString(number * 3)) FROM numbers(1000);
"

TABLE_FN="s3('${URL}/data_{1,2}.parquet', ${AUTH}, 'Parquet')"

QUERIES="
SELECT '-- object storage joined with MergeTree';
SELECT o.k, o.s, mt.heavy FROM ${TABLE_FN} AS o JOIN mt ON o.k = mt.k WHERE o.f = 3 ORDER BY o.k LIMIT 5;
SELECT '-- the object storage side can be unmatched';
SELECT mt.k, mt.heavy, o.s FROM mt LEFT JOIN ${TABLE_FN} AS o ON mt.k + 1500 = o.k ORDER BY mt.k LIMIT 3 OFFSET 165;
SELECT '-- object storage joined with itself';
SELECT a.k, a.s, b.s FROM ${TABLE_FN} AS a JOIN ${TABLE_FN} AS b ON a.k = b.k + 1 WHERE a.f = 5 ORDER BY a.k DESC LIMIT 4;
"

PLANS="
SELECT '-- lazy reads of object storage, of MergeTree';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%'), countIf(explain LIKE '%LazilyReadFromMergeTree%')
FROM (EXPLAIN SELECT o.k, o.s, mt.heavy FROM ${TABLE_FN} AS o JOIN mt ON o.k = mt.k WHERE o.f = 3 ORDER BY o.k LIMIT 5);
SELECT '-- lazy reads of object storage in the LEFT JOIN';
-- Only the object storage side is checked: where the top K is pushed below the join onto the MergeTree side, that
-- side reads the rows of the LIMIT only and is not read lazily.
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%')
FROM (EXPLAIN SELECT mt.k, mt.heavy, o.s FROM mt LEFT JOIN ${TABLE_FN} AS o ON mt.k + 1500 = o.k ORDER BY mt.k LIMIT 3 OFFSET 165);
"

# `s3_validate_etag_on_read` is pinned because a plain object storage read is reread lazily only with the
# ETag-pinned GET. The joins are pinned so that each side is read directly and keeps the order it is written in.
SETTINGS=(
    --s3_validate_etag_on_read=1
    --query_plan_optimize_lazy_materialization=1
    --query_plan_optimize_lazy_materialization_for_object_storage=1
    --query_plan_max_limit_for_lazy_materialization=0
    --enable_join_runtime_filters=0
    --query_plan_join_swap_table=0
    --query_plan_optimize_join_order_limit=0
    --enable_parallel_replicas=0
)

for enabled in 1 0; do
    echo "-- query_plan_lazy_materialization_for_join = $enabled"
    ${CLICKHOUSE_CLIENT} "${SETTINGS[@]}" --query_plan_lazy_materialization_for_join="$enabled" --query "$QUERIES"
done

${CLICKHOUSE_CLIENT} "${SETTINGS[@]}" --query_plan_lazy_materialization_for_join=1 --query "$PLANS"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mt"
