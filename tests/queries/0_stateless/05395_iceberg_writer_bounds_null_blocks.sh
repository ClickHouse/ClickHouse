#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_null_blocks"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# The writer takes the bounds of a data file from the min and max of every inserted block. A block
# that holds only NULLs in a column has no min or max there and must not change the column's bounds.
# 1000-row blocks: `n` is NULL in the first block, `m` in the last one, `z` everywhere.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
CREATE TABLE t (id Int64, n Nullable(Int64), m Nullable(Int64), z Nullable(Int64))
ENGINE = IcebergLocal('${ROOT}/t/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO t SELECT number, if(number < 1000, NULL, number), if(number >= 2000, NULL, number), NULL FROM numbers(3000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;
" < /dev/null

MANIFEST=$(find "${ROOT}/t/metadata" -name '*.avro' ! -name 'snap-*')
echo 'field, lower bound, upper bound, null count'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        field,
        if(has(mapKeys(lower), field), toString(reinterpretAsInt64(lower[field])), '-'),
        if(has(mapKeys(upper), field), toString(reinterpretAsInt64(upper[field])), '-'),
        nulls[field]
    FROM
    (
        SELECT
            mapFromArrays(arrayMap(x -> x.1, data_file.lower_bounds), arrayMap(x -> x.2, data_file.lower_bounds)) AS lower,
            mapFromArrays(arrayMap(x -> x.1, data_file.upper_bounds), arrayMap(x -> x.2, data_file.upper_bounds)) AS upper,
            mapFromArrays(arrayMap(x -> x.1, data_file.null_value_counts), arrayMap(x -> x.2, data_file.null_value_counts)) AS nulls
        FROM file('${MANIFEST}', 'Avro')
    )
    ARRAY JOIN [1, 2, 3, 4] AS field
    ORDER BY field"
