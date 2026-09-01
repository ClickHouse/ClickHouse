#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `IcebergLocal` needs the `USE_AVRO` build option.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BOUND_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_bound_width"

rm -rf "${BOUND_PATH}"

# One row, so there is exactly one data file and one manifest whatever the randomized block sizes are.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE bound_width (i32 Int32, d Date, i64 Int64, s String, d32 Date32)
    ENGINE = IcebergLocal('${BOUND_PATH}', 'Parquet') ORDER BY (i32);
    INSERT INTO bound_width SELECT 42, toDate('2024-06-01'), 100, 'abc', toDate32('1950-01-01');
"

echo '--- a manifest bound is as wide as its Iceberg type: `int` and `date` 4 bytes, `long` 8 ---'
for manifest in $(find "${BOUND_PATH}/metadata" -maxdepth 1 -name '*.avro' -not -name 'snap-*.avro' -type f | sort); do
    # Hex rather than a decoded value: `reinterpretAsInt32` reads its own width and returns the right
    # number from a bound of any length, so only the raw bytes pin the width. Field 1 is `i32`
    # (Iceberg `int`), 2 is `d` (`date`), 3 is `i64` (`long`), 4 is `s` (`string`). Field 5 is `d32`
    # (`date`, pre-epoch): the only bound here whose value is negative, so it pins the sign of the
    # narrowed write as well as its width.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            arraySort(arrayMap(x -> (x.1, hex(x.2)), tupleElement(data_file, 'lower_bounds'))) AS lower,
            arraySort(arrayMap(x -> (x.1, hex(x.2)), tupleElement(data_file, 'upper_bounds'))) AS upper
        FROM file('${manifest}', Avro)
        FORMAT Vertical;
    "
done

rm -rf "${BOUND_PATH}"
