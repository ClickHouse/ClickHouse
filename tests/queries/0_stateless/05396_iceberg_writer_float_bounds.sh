#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_float_bounds"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# One data file per `INSERT`. Iceberg keeps NaN out of the bounds of a float column and counts it in
# `nan_value_counts`, and a zero bound must be `-0.0` below and `+0.0` above. The last `INSERT` writes
# 1000-row blocks, the first of which holds only NaN in the float columns.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
CREATE TABLE t (id Int64, f64 Float64, f32 Float32, n Nullable(Float64))
ENGINE = IcebergLocal('${ROOT}/t/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO t SELECT number, number / 10, number / 10, number / 10 FROM numbers(1000);
INSERT INTO t SELECT number + 1000, 1000 + number / 10, 1000 + number / 10, 1000 + number / 10 FROM numbers(1000);
INSERT INTO t VALUES (2000, 0., 0., NULL), (2001, -0., -0., -0.);
INSERT INTO t VALUES (2002, 1., 1., NULL), (2003, nan, nan, nan), (2004, 3., 3., 3.);
INSERT INTO t VALUES (2005, nan, nan, NULL);
INSERT INTO t SELECT number + 3000, if(number < 1000, nan, number), if(number < 1000, nan, number),
    if(number < 500, NULL, if(number < 1000, nan, number)) FROM numbers(3000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;
" < /dev/null

MANIFESTS=$(find "${ROOT}/t/metadata" -name '*.avro' ! -name 'snap-*' -printf '%f,' | sed 's/,$//')
SOURCE="file('${ROOT}/t/metadata/{${MANIFESTS}}', 'Avro')"

echo '--- per data file (by its first id) and field (2 Float64, 3 Float32, 4 Nullable(Float64)): lower, upper, bound bytes, NaN count'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        first_id,
        field,
        if(has(mapKeys(lower), field), if(field = 3, toString(reinterpretAsFloat32(lower[field])), toString(reinterpretAsFloat64(lower[field]))), '-'),
        if(has(mapKeys(upper), field), if(field = 3, toString(reinterpretAsFloat32(upper[field])), toString(reinterpretAsFloat64(upper[field]))), '-'),
        if(has(mapKeys(lower), field), length(lower[field]), 0),
        if(has(mapKeys(nans), field), toString(nans[field]), '-')
    FROM
    (
        SELECT
            mapFromArrays(arrayMap(x -> x.1, data_file.lower_bounds), arrayMap(x -> x.2, data_file.lower_bounds)) AS lower,
            mapFromArrays(arrayMap(x -> x.1, data_file.upper_bounds), arrayMap(x -> x.2, data_file.upper_bounds)) AS upper,
            mapFromArrays(arrayMap(x -> x.1, data_file.nan_value_counts), arrayMap(x -> x.2, data_file.nan_value_counts)) AS nans,
            reinterpretAsInt64(lower[1]) AS first_id
        FROM ${SOURCE}
    )
    ARRAY JOIN [2, 3, 4] AS field
    ORDER BY first_id, field"

echo '--- the bytes of the zero bounds of f64'
${CLICKHOUSE_CLIENT} --query "
    SELECT hex(arrayFilter(x -> x.1 = 2, data_file.lower_bounds)[1].2), hex(arrayFilter(x -> x.1 = 2, data_file.upper_bounds)[1].2)
    FROM ${SOURCE}
    WHERE reinterpretAsInt64(arrayFilter(x -> x.1 = 1, data_file.lower_bounds)[1].2) = 2000"

echo '--- a range outside a file min/max-prunes it'
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM t WHERE f64 > 1050 SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_f64';
    SELECT count() FROM t WHERE f32 > 1050 SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_f32';
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${CLICKHOUSE_DATABASE}_f64', '${CLICKHOUSE_DATABASE}_f32')
    ORDER BY log_comment DESC"

echo '--- identity partitions on a Float64: the manifest-list summary holds the value, a NaN sets contains_nan'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
CREATE TABLE p (id Int64, f Float64) ENGINE = IcebergLocal('${ROOT}/p/', 'Parquet') PARTITION BY f
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO p VALUES (1, 1.5), (2, nan), (3, 100.);
SELECT count() FROM p WHERE f > 3;
SELECT count() FROM p WHERE NOT (f <= 3);
" < /dev/null
MANIFEST_LIST=$(find "${ROOT}/p/metadata" -name 'snap-*.avro' -printf '%f')
${CLICKHOUSE_CLIENT} --query "
    SELECT hex(assumeNotNull(s.lower_bound)), hex(assumeNotNull(s.upper_bound)), s.contains_nan
    FROM file('${ROOT}/p/metadata/${MANIFEST_LIST}', 'Avro')
    ARRAY JOIN partitions AS s
    ORDER BY 1"
