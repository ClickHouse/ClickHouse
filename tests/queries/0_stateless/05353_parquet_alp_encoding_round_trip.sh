#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not built in the fast test.

# Round trips of `Float32` and `Float64` columns written with the `ALP` encoding
# (`output_format_parquet_use_alp_encoding`), compared bit-exactly with the source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_alp"
trap 'rm -f "${P}"_*.parquet' EXIT

# The same source data in every invocation of clickhouse-local: decimal-like values, values that
# can only be stored as exceptions, NaN (also with a payload), infinities, -0.0, denormals, values
# out of range of the integer types, constants, nullables and arrays. 10007 rows is not a multiple
# of the vector size. (The NaNs with payloads are not constants, because `multiIf` merges the
# constant NaNs of its branches.)
SOURCE="
CREATE TABLE src ENGINE = Memory AS SELECT
    number AS id,
    round(number * 0.37 - 1000, 2) AS d_decimal,
    toFloat32(round(number * 0.37 - 1000, 2)) AS f_decimal,
    multiIf(
        number % 97 = 0, nan,
        number % 89 = 0, inf,
        number % 83 = 0, -inf,
        number % 79 = 0, -0.0,
        number % 73 = 0, 5e-324,
        number % 71 = 0, sqrt(number),
        number % 67 = 0, reinterpretAsFloat64(toUInt64(0x7FF800DEADBEEF00) + 0 * number),
        number % 61 = 0, reinterpretAsFloat64(toUInt64(0xFFF8000000000001) + 0 * number),
        number % 59 = 0, 1e300,
        number % 53 = 0, 2.2250738585072014e-308,
        round(number / 100, 1)) AS d_special,
    multiIf(
        number % 97 = 0, toFloat32(nan),
        number % 89 = 0, toFloat32(inf),
        number % 83 = 0, toFloat32(-inf),
        number % 79 = 0, toFloat32(-0.0),
        number % 73 = 0, reinterpretAsFloat32(toUInt32(1 + 0 * number)),
        number % 71 = 0, toFloat32(sqrt(number)),
        number % 67 = 0, reinterpretAsFloat32(toUInt32(0x7FC0DEAD + 0 * number)),
        number % 59 = 0, toFloat32(3e38),
        toFloat32(round(number / 100, 1))) AS f_special,
    sqrt(number + 1) * pi() AS d_random,
    toFloat32(sqrt(number + 1) * pi()) AS f_random,
    toFloat64(number) * 1e15 - 5e18 AS d_large,
    7.77 AS d_const,
    toFloat32(7.77) AS f_const,
    if(number % 3 = 0, NULL, number / 4) AS d_nullable,
    if(number % 5 = 0, NULL, toFloat32(number / 8)) AS f_nullable,
    arrayMap(i -> round(i * 1.5 + number / 10, 1), range(number % 5)) AS d_array,
    arrayMap(i -> if(i = 1, NULL, toFloat32(i * 0.25 + number)), range(number % 4)) AS f_array
FROM numbers(10007);
"

# Fingerprint of the bits of all values, independent of the row order.
FINGERPRINT="(count(), sum(cityHash64(
    id,
    reinterpretAsUInt64(d_decimal), reinterpretAsUInt32(f_decimal),
    reinterpretAsUInt64(d_special), reinterpretAsUInt32(f_special),
    reinterpretAsUInt64(d_random), reinterpretAsUInt32(f_random),
    reinterpretAsUInt64(d_large), reinterpretAsUInt64(d_const), reinterpretAsUInt32(f_const),
    isNull(d_nullable), reinterpretAsUInt64(ifNull(d_nullable, 0)),
    isNull(f_nullable), reinterpretAsUInt32(ifNull(f_nullable, 0)),
    arrayMap(x -> reinterpretAsUInt64(x), d_array),
    arrayMap(x -> (isNull(x), reinterpretAsUInt32(ifNull(x, 0))), f_array))))"

ENCODINGS="arrayMap(c -> (tupleElement(c, 'path'), tupleElement(c, 'encodings')), columns)"

echo "--- default page size"
$CLICKHOUSE_LOCAL -q "
$SOURCE
INSERT INTO FUNCTION file('${P}_default.parquet') SELECT * FROM src SETTINGS output_format_parquet_use_alp_encoding = 1;
SELECT (SELECT $FINGERPRINT FROM src) = (SELECT $FINGERPRINT FROM file('${P}_default.parquet'));
SELECT $ENCODINGS FROM file('${P}_default.parquet', ParquetMetadata) FORMAT Vertical;
"

echo "--- small pages and row groups"
$CLICKHOUSE_LOCAL -q "
$SOURCE
INSERT INTO FUNCTION file('${P}_small.parquet') SELECT * FROM src SETTINGS
    output_format_parquet_use_alp_encoding = 1, output_format_parquet_data_page_size = 1000,
    output_format_parquet_batch_size = 100, output_format_parquet_row_group_size = 3000;
SELECT num_row_groups FROM file('${P}_small.parquet', ParquetMetadata);
SELECT (SELECT $FINGERPRINT FROM src) = (SELECT $FINGERPRINT FROM file('${P}_small.parquet'));

-- Exact bits of the special values.
SELECT id, hex(reinterpretAsUInt64(d_special)), hex(reinterpretAsUInt32(f_special))
FROM file('${P}_small.parquet') WHERE id IN (0, 1, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 10006) ORDER BY id;

-- Filtering and skipping in the middle of pages and vectors.
SELECT
    (SELECT sum(cityHash64(id, reinterpretAsUInt64(d_special), reinterpretAsUInt32(f_decimal))) FROM src WHERE id % 7 = 3),
    (SELECT sum(cityHash64(id, reinterpretAsUInt64(d_special), reinterpretAsUInt32(f_decimal))) FROM file('${P}_small.parquet') WHERE id % 7 = 3);
SELECT
    (SELECT groupArray(reinterpretAsUInt64(d_decimal)) FROM (SELECT d_decimal FROM src ORDER BY id LIMIT 5 OFFSET 5000)),
    (SELECT groupArray(reinterpretAsUInt64(d_decimal)) FROM (SELECT d_decimal FROM file('${P}_small.parquet') ORDER BY id LIMIT 5 OFFSET 5000));

-- Statistics, page index and bloom filters are still usable.
SELECT count(), min(id) FROM file('${P}_small.parquet') WHERE d_decimal > 2000 SETTINGS input_format_parquet_filter_push_down = 1;
SELECT count(), min(id) FROM file('${P}_small.parquet') WHERE d_decimal = -963 SETTINGS input_format_parquet_bloom_filter_push_down = 1;
SELECT count(), min(id) FROM file('${P}_small.parquet') WHERE f_decimal = toFloat32(-963);
SELECT count(), min(id) FROM file('${P}_small.parquet') WHERE d_nullable IS NULL;
"

echo "--- empty and tiny files"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('${P}_empty.parquet') SELECT toFloat64(number) AS d, toFloat32(number) AS f FROM numbers(0) SETTINGS output_format_parquet_use_alp_encoding = 1;
SELECT count() FROM file('${P}_empty.parquet');
INSERT INTO FUNCTION file('${P}_null.parquet') SELECT CAST(NULL, 'Nullable(Float64)') AS d FROM numbers(3) SETTINGS output_format_parquet_use_alp_encoding = 1;
SELECT d FROM file('${P}_null.parquet');
INSERT INTO FUNCTION file('${P}_one.parquet') SELECT 0.5 AS d FROM numbers(1) SETTINGS output_format_parquet_use_alp_encoding = 1;
SELECT d FROM file('${P}_one.parquet');
"

echo "--- ALP is smaller than PLAIN for decimal-like values (but not much for Float32, which has many exceptions)"
$CLICKHOUSE_LOCAL -q "
$SOURCE
INSERT INTO FUNCTION file('${P}_plain.parquet') SELECT d_decimal, f_decimal FROM src SETTINGS
    output_format_parquet_compression_method = 'none', output_format_parquet_max_dictionary_size = 0;
INSERT INTO FUNCTION file('${P}_alp.parquet') SELECT d_decimal, f_decimal FROM src SETTINGS
    output_format_parquet_compression_method = 'none', output_format_parquet_use_alp_encoding = 1;
SELECT $ENCODINGS FROM file('${P}_plain.parquet', ParquetMetadata);
SELECT $ENCODINGS FROM file('${P}_alp.parquet', ParquetMetadata);
SELECT
    arrayMap(c -> tupleElement(c, 'total_compressed_size'), (SELECT columns FROM file('${P}_alp.parquet', ParquetMetadata))) AS alp,
    arrayMap(c -> tupleElement(c, 'total_compressed_size'), (SELECT columns FROM file('${P}_plain.parquet', ParquetMetadata))) AS plain,
    arrayMap((a, p) -> a < p, alp, plain);
"
