#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CLICKHOUSE_TMP"

$CLICKHOUSE_LOCAL -n <<'SQL'
SET engine_file_truncate_on_insert = 1;

SELECT 'Simple Table';

DROP TABLE IF EXISTS buf_simple;

CREATE TABLE buf_simple
(
    id UInt64,
    x  Int32,
    y  Float64
)
ENGINE = Memory;

INSERT INTO buf_simple VALUES
    (1,  10,   0.5),
    (2, -20,  -1.25),
    (3,   0,  42.0);

SELECT * FROM buf_simple FORMAT HASH;

SELECT *
FROM buf_simple
ORDER BY id;

SELECT *
FROM buf_simple
ORDER BY id
INTO OUTFILE '03743_buffers_simple.buffers' TRUNCATE
FORMAT Buffers;

TRUNCATE TABLE buf_simple;

INSERT INTO buf_simple
FROM INFILE '03743_buffers_simple.buffers'
FORMAT Buffers;

SELECT * FROM buf_simple FORMAT HASH;

SELECT *
FROM buf_simple
ORDER BY id;


SELECT 'Simple table with many blocks';

SET max_block_size = 1000;

SELECT
    number AS id,
    number % 3 AS k
FROM numbers(1000000)
INTO OUTFILE '03743_buffers_numbers.buffers' TRUNCATE
FORMAT Buffers;

SELECT
    count() AS cnt,
    sum(id) AS sum_id,
    sum(k) AS sum_k
FROM file(
    '03743_buffers_numbers.buffers',
    'Buffers',
    'id UInt64, k UInt8'
);

SELECT
    *
FROM file(
    '03743_buffers_numbers.buffers',
    'Buffers',
    'id UInt64, k UInt8'
)
ORDER BY id
LIMIT -5;

SELECT 'Complex types';
SELECT
    if(number % 3 = 0, NULL, number) AS n_nullable,
    number AS id,
    toString(number) AS s,
    [number, number + 1] AS arr,
    (number, toString(number)) AS tup
FROM numbers(1000000)
INTO OUTFILE '03743_buffers_complex.buffers' TRUNCATE
FORMAT Buffers;

SELECT
    sum(id)                            AS sum_id,
    count()                            AS cnt,
    sumOrNull(n_nullable)              AS sum_n_nullable,
    sum(length(s))                     AS sum_len_s,
    sum(length(arr))                   AS sum_len_arr
FROM file(
    '03743_buffers_complex.buffers',
    'Buffers',
    'n_nullable Nullable(UInt64), id UInt64, s String, arr Array(UInt64), tup Tuple(UInt64, String)'
);

SELECT
    sum(tupleElement(tup, 1))                  AS sum_tup_1,
    sum(length(tupleElement(tup, 2)))          AS sum_len_tup_2
FROM file(
    '03743_buffers_complex.buffers',
    'Buffers',
    'n_nullable Nullable(UInt64), id UInt64, s String, arr Array(UInt64), tup Tuple(UInt64, String)'
);

SELECT 'Empty table';
DROP TABLE IF EXISTS buf_empty;
CREATE TABLE buf_empty (id UInt64) ENGINE = Memory;

SELECT *
FROM buf_empty
WHERE 0
INTO OUTFILE '03743_buffers_zero_rows.buffers' TRUNCATE
FORMAT Buffers;

SELECT count()
FROM file('03743_buffers_zero_rows.buffers', 'Buffers', 'id UInt64');


SELECT 'Constant columns';
SELECT
    'x'    AS s,
    number AS k
FROM numbers(10)
INTO OUTFILE '03743_buffers_const.buffers' TRUNCATE
FORMAT Buffers;

SELECT
    sum(k)       AS sum_k,
    groupArray(s) AS arr_s
FROM file(
    '03743_buffers_const.buffers',
    'Buffers',
    's String, k UInt64'
);

SELECT 'Buffers via file() source and INSERT';
INSERT INTO TABLE FUNCTION
    file('03743_buffers_via_file.buffers', 'Buffers', 'id UInt64, name String')
SELECT
    number AS id,
    concat('name_', toString(number)) AS name
FROM numbers(10);

SELECT *
FROM file(
    '03743_buffers_via_file.buffers',
    'Buffers',
    'id UInt64, name String'
)
ORDER BY id;

SELECT 'JSON, Variant, Dynamic';
DROP TABLE IF EXISTS buf_json_variant_dynamic;

CREATE TABLE buf_json_variant_dynamic
(
    j  JSON(a.b UInt32, SKIP a.e),
    v  Variant(UInt64, String, Array(UInt64)),
    d  Dynamic
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO buf_json_variant_dynamic VALUES
(
    '{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2025-01-01"}',
    42,
    42
),
(
    '{"f" : "Hello", "d" : "2025-01-02"}',
    'Hello, World!',
    'Hello, World!'
),
(
    '{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}',
    [1, 2, 3],
    [1, 2, 3]
);

SELECT *
FROM buf_json_variant_dynamic
ORDER BY tuple()
INTO OUTFILE '03743_buffers_json_variant_dynamic.buffers' TRUNCATE
FORMAT Buffers;

SELECT * FROM buf_json_variant_dynamic FORMAT HASH;

SELECT * FROM buf_json_variant_dynamic;

TRUNCATE TABLE buf_json_variant_dynamic;

INSERT INTO buf_json_variant_dynamic
FROM INFILE '03743_buffers_json_variant_dynamic.buffers'
FORMAT Buffers;

SELECT * FROM buf_json_variant_dynamic FORMAT HASH;

SELECT * FROM buf_json_variant_dynamic;

SELECT
    count()                                          AS cnt_rows,
    sum(j.a.b)                                       AS sum_json_ab,
    sumIf(length(j.c), isNotNull(j.c))               AS sum_len_json_c,
    countIf(isNotNull(j.d))                          AS cnt_json_d,
    sumOrNull(v.UInt64)                              AS sum_v_uint,
    sum(arraySum(v.`Array(UInt64)`))                 AS sum_v_arr,
    arraySort(groupUniqArray(variantType(v)))        AS v_types,
    sumOrNull(d.Int64)                               AS sum_d_int,
    sum(arraySum(d.`Array(Int64)`))                  AS sum_d_arr,
    arraySort(groupUniqArray(dynamicType(d)))        AS d_types,
    arraySort(groupUniqArray(JSONAllPathsWithTypes(j))) AS json_paths_and_types
FROM buf_json_variant_dynamic;

SELECT
    count()                                          AS cnt_rows,
    sum(j.a.b)                                       AS sum_json_ab,
    sumIf(length(j.c), isNotNull(j.c))               AS sum_len_json_c,
    countIf(isNotNull(j.d))                          AS cnt_json_d,
    sumOrNull(v.UInt64)                              AS sum_v_uint,
    sum(arraySum(v.`Array(UInt64)`))                 AS sum_v_arr,
    arraySort(groupUniqArray(variantType(v)))        AS v_types,
    sumOrNull(d.Int64)                               AS sum_d_int,
    sum(arraySum(d.`Array(Int64)`))                  AS sum_d_arr,
    arraySort(groupUniqArray(dynamicType(d)))        AS d_types,
    arraySort(groupUniqArray(JSONAllPathsWithTypes(j))) AS json_paths_and_types
FROM file(
    '03743_buffers_json_variant_dynamic.buffers',
    'Buffers',
    'j JSON(a.b UInt32, SKIP a.e), v Variant(UInt64, String, Array(UInt64)), d Dynamic'
);


SELECT 'LowCardinality / Nullable / Array';

DROP TABLE IF EXISTS buf_lc;

CREATE TABLE buf_lc
(
    lc_str LowCardinality(String),
    lc_nullable LowCardinality(Nullable(String)),
    lc_arr Array(LowCardinality(String))
)
ENGINE = Memory;

INSERT INTO buf_lc VALUES
    ('a', 'a', ['a', 'b']),
    ('b', NULL, ['b', 'c']),
    ('a', 'c', ['a', 'c']);

SELECT *
FROM buf_lc
ORDER BY tuple()
INTO OUTFILE '03743_buffers_lc.buffers' TRUNCATE
FORMAT Buffers;

SELECT * FROM buf_lc FORMAT HASH;

TRUNCATE TABLE buf_lc;

INSERT INTO buf_lc
FROM INFILE '03743_buffers_lc.buffers'
FORMAT Buffers;

SELECT * FROM buf_lc FORMAT HASH;

SELECT
    count()                                  AS cnt_rows,
    groupUniqArray(lc_str)                   AS uniq_lc_str,
    countIf(isNull(lc_nullable))             AS cnt_null_lc_nullable,
    groupUniqArray(arraySort(lc_arr))        AS uniq_lc_arr_sorted
FROM buf_lc;


SELECT 'Decimal / Date / DateTime64';

DROP TABLE IF EXISTS buf_decimal_datetime;

CREATE TABLE buf_decimal_datetime
(
    d10  Decimal(10, 3),
    d18  Decimal(18, 6),
    dt32 Date,
    dt64 DateTime64(3, 'UTC')
)
ENGINE = Memory;

INSERT INTO buf_decimal_datetime VALUES
(123.456,  1.000001, toDate('2025-01-01'), toDateTime64('2025-01-01 00:00:00.123', 3, 'UTC')),
(-7.500,   0.000001, toDate('2025-01-02'), toDateTime64('2025-01-02 12:34:56.789', 3, 'UTC')),
(0.000,    42.424242, toDate('2025-01-03'), toDateTime64('2025-01-03 23:59:59.000', 3, 'UTC'));

SELECT *
FROM buf_decimal_datetime
ORDER BY tuple()
INTO OUTFILE '03743_buffers_decimal_datetime.buffers' TRUNCATE
FORMAT Buffers;

SELECT * FROM buf_decimal_datetime FORMAT HASH;

TRUNCATE TABLE buf_decimal_datetime;

INSERT INTO buf_decimal_datetime
FROM INFILE '03743_buffers_decimal_datetime.buffers'
FORMAT Buffers;

SELECT * FROM buf_decimal_datetime FORMAT HASH;

SELECT
    count()                      AS cnt_rows,
    sum(d10)                     AS sum_d10,
    sum(d18)                     AS sum_d18,
    min(dt32)                    AS min_dt32,
    max(dt32)                    AS max_dt32,
    min(dt64)                    AS min_dt64,
    max(dt64)                    AS max_dt64
FROM buf_decimal_datetime;

SELECT 'Nested / Map-like';

DROP TABLE IF EXISTS buf_nested;

CREATE TABLE buf_nested
(
    n Nested
    (
        k UInt8,
        v String
    ),
    m Map(String, UInt64)
)
ENGINE = Memory;

INSERT INTO buf_nested VALUES
(
    [1, 2], ['a', 'b'],
    map('x', 10, 'y', 20)
),
(
    [3], ['c'],
    map('y', 5, 'z', 7)
);

SELECT *
FROM buf_nested
ORDER BY tuple()
INTO OUTFILE '03743_buffers_nested.buffers' TRUNCATE
FORMAT Buffers;

SELECT * FROM buf_nested FORMAT HASH;

TRUNCATE TABLE buf_nested;

INSERT INTO buf_nested
FROM INFILE '03743_buffers_nested.buffers'
FORMAT Buffers;

SELECT * FROM buf_nested FORMAT HASH;

SELECT
    count()                           AS cnt_rows,
    arraySort(groupUniqArray(n.k))    AS uniq_nested_k,
    arraySort(groupUniqArray(n.v))    AS uniq_nested_v,
    sum(m['x'])                       AS sum_m_x,
    sum(m['y'])                       AS sum_m_y,
    sum(m['z'])                       AS sum_m_z
FROM buf_nested;
SQL

rm -f 03743_buffers_*.buffers
