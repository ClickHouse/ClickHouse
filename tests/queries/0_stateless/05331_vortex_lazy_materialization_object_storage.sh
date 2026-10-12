#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds; the test also reads from
# Minio and needs `IcebergLocal` (the USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for Vortex files in object storage and in Iceberg tables: for
# `ORDER BY ... LIMIT n` queries, the columns that are not needed for sorting and filtering are
# read only for the `n` rows that survive the `LIMIT`. Every query is run with the optimization
# enabled and disabled; the results must match.

URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_vortex_lazy_mat"
AUTH="'test', 'testtest'"

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05331_vortex_lazy_mat_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT
# `clickhouse-local` takes its working directory for the user files directory.
cd "${LOCAL_DIR}" || exit 1

ICEBERG_TABLE="CREATE TABLE t ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');"

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    INSERT INTO FUNCTION s3('${URL}/data_1.vortex', ${AUTH}, 'Vortex')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s FROM numbers(0, 1000)
    SETTINGS s3_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3('${URL}/data_2.vortex', ${AUTH}, 'Vortex')
    SELECT number AS k, number % 17 AS f, concat('val_', toString(number)) AS s FROM numbers(1000, 1000)
    SETTINGS s3_truncate_on_insert = 1;

    CREATE TABLE t (k UInt64, f UInt64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');
    INSERT INTO t SELECT number AS k, number % 7 AS f, concat('val_', toString(number)) AS s FROM numbers(5000);
    INSERT INTO t SELECT number AS k, number % 7 AS f, concat('val_', toString(number)) AS s FROM numbers(5000, 5000);
"

S3_TABLE="s3('${URL}/data_{1,2}.vortex', ${AUTH}, 'Vortex')"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer, and some CI
# configurations run with the old analyzer. `s3_validate_etag_on_read` is pinned because for plain
# (non-data-lake) object storage the lazy reread is only generation-safe with the ETag-pinned GET.
run()
{
    local enabled=$1
    local query=$2
    ${CLICKHOUSE_LOCAL} \
        --enable_analyzer=1 \
        --s3_validate_etag_on_read=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
        --query "${ICEBERG_TABLE} ${query}"
}

QUERIES="
SELECT '-- s3: ORDER BY ... LIMIT';
SELECT k, s FROM ${S3_TABLE} ORDER BY k LIMIT 3;
SELECT '-- s3: ORDER BY ... DESC LIMIT across files';
SELECT k, s FROM ${S3_TABLE} ORDER BY k DESC LIMIT 3;
SELECT '-- s3: with a filter';
SELECT k, s FROM ${S3_TABLE} WHERE f = 3 ORDER BY k DESC LIMIT 3;
SELECT '-- s3: virtual columns together with lazy columns';
SELECT _file, _row_number, k, s FROM ${S3_TABLE} ORDER BY k LIMIT 2 OFFSET 999;
SELECT '-- Iceberg: ORDER BY ... LIMIT';
SELECT k, s FROM t ORDER BY k LIMIT 3;
SELECT '-- Iceberg: ORDER BY ... DESC LIMIT across data files';
SELECT k, s FROM t ORDER BY k DESC LIMIT 3;
SELECT '-- Iceberg: with a filter';
SELECT k, s FROM t WHERE f = 3 AND k BETWEEN 4990 AND 5010 ORDER BY k LIMIT 3;
"

for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_object_storage = $enabled"
    run "$enabled" "$QUERIES"
    echo '-- the number of lazy read steps in the plan'
    run "$enabled" "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${S3_TABLE} ORDER BY k LIMIT 3)"
    run "$enabled" "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM t ORDER BY k LIMIT 3)"
done
