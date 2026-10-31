-- With `nullable_serialization_version = 'compact_null_map'` the null map of a granule without NULLs (or with only NULLs)
-- takes a few bytes. The data must be read the same as with the basic serialization, in Wide and Compact parts,
-- including subcolumns, reading from the middle of the granules, merges and mutations, and via the native protocol.

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_basic;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;

CREATE TABLE t_basic (id UInt64, x Nullable(UInt64), s Nullable(String), arr Array(Nullable(UInt32)), tup Tuple(a Nullable(UInt8), b String), nt Nullable(Tuple(a UInt32, b LowCardinality(String))))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', nullable_serialization_version = 'basic', min_bytes_for_wide_part = 0;

CREATE TABLE t_wide AS t_basic
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', nullable_serialization_version = 'compact_null_map', min_bytes_for_wide_part = 0;

CREATE TABLE t_compact AS t_basic
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', nullable_serialization_version = 'compact_null_map', min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

-- The granules of `x`: 0-4 without NULLs, 5-9 only NULLs, 10-14 with some NULLs, 15-19 without NULLs.
INSERT INTO t_basic SELECT
    number,
    if(intDiv(number, 100) BETWEEN 5 AND 9 OR (intDiv(number, 100) BETWEEN 10 AND 14 AND number % 3 = 0), NULL, number),
    if(number % 250 = 7, NULL, toString(number % 17)),
    [if(number % 7 = 0, NULL, number), number],
    (if(number % 11 = 0, NULL, number % 256), toString(number)),
    if(number % 5 = 0, NULL, (number, toString(number % 3)))
FROM numbers(2000);
INSERT INTO t_basic SELECT
    2000 + number,
    if(number % 2 = 0, NULL, number),
    toString(number),
    [number],
    (number % 256, ''),
    (number, 'x')
FROM numbers(500);

INSERT INTO t_wide SELECT * FROM t_basic ORDER BY id;
INSERT INTO t_compact SELECT * FROM t_basic ORDER BY id;

SELECT 'the parts';
SELECT table, part_type, count() FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table, part_type ORDER BY table;

SELECT 'full read';
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide;
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact;

SELECT 'subcolumns';
SELECT 'basic', sum(x.null), sum(s.null), sum(s.size), sum(arraySum(arr.null)), sum(tup.a.null), sum(nt.null), sum(nt.a), countIf(nt.b IS NULL), countIf(x IS NULL) FROM t_basic;
SELECT 'wide', sum(x.null), sum(s.null), sum(s.size), sum(arraySum(arr.null)), sum(tup.a.null), sum(nt.null), sum(nt.a), countIf(nt.b IS NULL), countIf(x IS NULL) FROM t_wide;
SELECT 'compact', sum(x.null), sum(s.null), sum(s.size), sum(arraySum(arr.null)), sum(tup.a.null), sum(nt.null), sum(nt.a), countIf(nt.b IS NULL), countIf(x IS NULL) FROM t_compact;

SELECT 'a subcolumn and the column together';
SELECT 'basic', sum(x.null), count(x), sum(cityHash64(toString(nt))), sum(nt.null) FROM t_basic;
SELECT 'wide', sum(x.null), count(x), sum(cityHash64(toString(nt))), sum(nt.null) FROM t_wide;
SELECT 'compact', sum(x.null), count(x), sum(cityHash64(toString(nt))), sum(nt.null) FROM t_compact;

SELECT 'reading from the middle of the data';
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic WHERE id BETWEEN 250 AND 349 OR id BETWEEN 1250 AND 1449 OR id >= 2450;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide WHERE id BETWEEN 250 AND 349 OR id BETWEEN 1250 AND 1449 OR id >= 2450;
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact WHERE id BETWEEN 250 AND 349 OR id BETWEEN 1250 AND 1449 OR id >= 2450;
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic PREWHERE x IS NULL WHERE s != '3';
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide PREWHERE x IS NULL WHERE s != '3';
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact PREWHERE x IS NULL WHERE s != '3';
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic SETTINGS max_block_size = 37, max_threads = 1;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide SETTINGS max_block_size = 37, max_threads = 1;
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact SETTINGS max_block_size = 37, max_threads = 1;
SELECT id, x, s, arr, tup, nt FROM t_wide WHERE id IN (0, 499, 500, 1002, 1003, 1999, 2000, 2001) ORDER BY id;
SELECT id, x, s, arr, tup, nt FROM t_compact WHERE id IN (0, 499, 500, 1002, 1003, 1999, 2000, 2001) ORDER BY id;

SELECT 'the native protocol';
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM remote('127.0.0.{1,2}', currentDatabase(), t_basic);
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM remote('127.0.0.{1,2}', currentDatabase(), t_wide);
SELECT id, x, s, arr, tup, nt FROM remote('127.0.0.{1,2}', currentDatabase(), t_wide) WHERE id IN (499, 500, 1003) ORDER BY id;

SELECT 'merge';
OPTIMIZE TABLE t_basic FINAL;
OPTIMIZE TABLE t_wide FINAL;
OPTIMIZE TABLE t_compact FINAL;
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide;
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact;

SELECT 'mutation';
ALTER TABLE t_basic UPDATE x = NULL, s = NULL WHERE id % 13 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_wide UPDATE x = NULL, s = NULL WHERE id % 13 = 0 SETTINGS mutations_sync = 2;
ALTER TABLE t_compact UPDATE x = NULL, s = NULL WHERE id % 13 = 0 SETTINGS mutations_sync = 2;
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide;
SELECT 'compact', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_compact;

SELECT 'parts of both versions';
ALTER TABLE t_basic MODIFY SETTING nullable_serialization_version = 'compact_null_map';
INSERT INTO t_basic SELECT 3000 + number, if(number % 2 = 0, NULL, number), NULL, [], (NULL, ''), NULL FROM numbers(300);
INSERT INTO t_wide SELECT 3000 + number, if(number % 2 = 0, NULL, number), NULL, [], (NULL, ''), NULL FROM numbers(300);
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic;
SELECT 'wide', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_wide;
OPTIMIZE TABLE t_basic FINAL;
SELECT 'basic', count(), sum(cityHash64(toString((id, x, s, arr, tup, nt)))) FROM t_basic;

DROP TABLE t_basic;
DROP TABLE t_wide;
DROP TABLE t_compact;
