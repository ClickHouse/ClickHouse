#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not built in the fast test.

# Reading `ALP` encoded columns written by another implementation:
# `alp_extended.zstd.parquet` from https://github.com/apache/parquet-testing/blob/master/data/alp_extended.zstd.parquet
# (commit 09f3cdbde45302f0f0c689c950e465e98a9df960, written with the Arrow C++ implementation).
# All its columns hold the same 9032 values, the `*_plain` ones in `PLAIN`, the `*_alp_*` ones in `ALP`
# with vectors of 1024, 4096 and 32 values. The decoded values must be bit-exactly the `PLAIN` ones.
# The data is described in https://github.com/apache/parquet-testing/blob/master/data/README.md#alp-encoding

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

F="$CUR_DIR/data_parquet/alp_extended.zstd.parquet"

$CLICKHOUSE_LOCAL -q "
DESCRIBE file('$F');
SELECT arrayMap(c -> (tupleElement(c, 'path'), tupleElement(c, 'encodings')), columns) FROM file('$F', ParquetMetadata) FORMAT Vertical;

SELECT
    count(),
    countIf(float_plain IS NULL),
    countIf(isNaN(float_plain)),
    countIf(isNull(float_alp_1024) != isNull(float_plain) OR reinterpretAsUInt32(ifNull(float_alp_1024, 0)) != reinterpretAsUInt32(ifNull(float_plain, 0))),
    countIf(isNull(float_alp_4096) != isNull(float_plain) OR reinterpretAsUInt32(ifNull(float_alp_4096, 0)) != reinterpretAsUInt32(ifNull(float_plain, 0))),
    countIf(isNull(float_alp_32) != isNull(float_plain) OR reinterpretAsUInt32(ifNull(float_alp_32, 0)) != reinterpretAsUInt32(ifNull(float_plain, 0))),
    countIf(isNull(double_alp_1024) != isNull(double_plain) OR reinterpretAsUInt64(ifNull(double_alp_1024, 0)) != reinterpretAsUInt64(ifNull(double_plain, 0))),
    countIf(isNull(double_alp_4096) != isNull(double_plain) OR reinterpretAsUInt64(ifNull(double_alp_4096, 0)) != reinterpretAsUInt64(ifNull(double_plain, 0))),
    countIf(isNull(double_alp_32) != isNull(double_plain) OR reinterpretAsUInt64(ifNull(double_alp_32, 0)) != reinterpretAsUInt64(ifNull(double_plain, 0)))
FROM file('$F');

-- The values from the description of the file, by row number.
SELECT
    _row_number,
    hex(reinterpretAsUInt64(double_alp_1024)), hex(reinterpretAsUInt64(double_alp_4096)), hex(reinterpretAsUInt64(double_alp_32)),
    hex(reinterpretAsUInt32(float_alp_1024)), hex(reinterpretAsUInt32(float_alp_4096)), hex(reinterpretAsUInt32(float_alp_32)),
    double_alp_1024, float_alp_32
FROM file('$F')
WHERE _row_number IN (0, 1023, 1024, 1500, 2000, 2001, 2002, 2003, 2047, 2500, 3100, 3711, 6144, 7168, 8199, 8200, 8999, 9000, 9001, 9031)
ORDER BY _row_number;

-- Decoding with a filter, and skipping parts of vectors and whole vectors.
SELECT
    countIf(reinterpretAsUInt64(ifNull(double_alp_32, 0)) != reinterpretAsUInt64(ifNull(double_plain, 0))),
    countIf(reinterpretAsUInt32(ifNull(float_alp_4096, 0)) != reinterpretAsUInt32(ifNull(float_plain, 0))),
    count()
FROM file('$F') WHERE _row_number % 7 = 2;

SELECT count(), sum(cityHash64(reinterpretAsUInt64(double_alp_1024))) = sum(cityHash64(reinterpretAsUInt64(double_plain)))
FROM file('$F') WHERE double_plain > 9.5;
"

# `alp_arrow_rs_v2.parquet` was written by arrow-rs (https://github.com/apache/arrow-rs, commit dfef34e6, `ArrowWriter`
# with `WriterVersion::PARQUET_2_0`, i.e. data pages v2, no dictionary, no compression, `Encoding::ALP` for all
# columns, at most 700 rows per page and 3000 rows per row group). For row `i` of 5000:
#  * `d Float64`: NaN if i % 100 = 99, -0.0 if i = 7, +Inf if i = 13, otherwise `i * 0.01 - 50`;
#  * `f Nullable(Float32)`: NULL if i % 3 = 0, otherwise `f32(i) * 0.5`;
#  * `l Array(Float64)`: `[i + j * 0.25 for j in range(i % 4)]`.
G="$CUR_DIR/data_parquet/alp_arrow_rs_v2.parquet"

$CLICKHOUSE_LOCAL -q "
DESCRIBE file('$G');
SELECT arrayMap(c -> (tupleElement(c, 'path'), tupleElement(c, 'encodings')), columns) FROM file('$G', ParquetMetadata);
SELECT
    count(),
    countIf(reinterpretAsUInt64(d) != reinterpretAsUInt64(multiIf(i % 100 = 99, nan, i = 7, -0.0, i = 13, inf, i * 0.01 - 50))),
    countIf(isNull(f) != (i % 3 = 0) OR (f IS NOT NULL AND reinterpretAsUInt32(f) != reinterpretAsUInt32(toFloat32(i * 0.5)))),
    countIf(arrayMap(x -> reinterpretAsUInt64(assumeNotNull(x)), l) != arrayMap(j -> reinterpretAsUInt64(i + j * 0.25), range(i % 4))),
    countIf(arrayExists(x -> x IS NULL, l)),
    sum(length(l))
FROM (SELECT *, _row_number AS i FROM file('$G'));
SELECT count(), sum(f), sum(arraySum(x -> ifNull(x, 0), l)) FROM file('$G') WHERE _row_number % 11 = 5;
"
