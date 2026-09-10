-- Two streams of one column whose files differ (`c.size0` and `c%2Esize0` for Array(Tuple(`size0` UInt64)))
-- used to share a substreams cache slot, so one of them got the other's column. The data on disk is correct.
-- The wide reader uses the cache for every read, the compact reader only for subcolumn reads.

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_cache_key;

CREATE TABLE t_cache_key (c Array(Tuple(`size0` UInt64)), d Tuple(`a` String, `a.size` UInt64))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_cache_key VALUES ([(100), (200)], ('abc', 99)), ([(300)], ('de', 98)), ([], ('', 97));
SELECT c, d FROM t_cache_key;

-- A merge reads the parts the same way.
INSERT INTO t_cache_key VALUES ([(400)], ('f', 96));
OPTIMIZE TABLE t_cache_key FINAL;
SELECT count() FROM t_cache_key;

DROP TABLE t_cache_key;

DROP TABLE IF EXISTS t_cache_wide;
DROP TABLE IF EXISTS t_cache_compact;

-- Every column is wrapped in a Tuple: reading `<column>.x` is the only case in which the compact reader
-- allocates a substreams cache.
CREATE TABLE t_cache_wide
(
    -- A Tuple element named like the array sizes of an enclosing Array.
    c1 Tuple(x Array(Tuple(`size0` UInt64)), y UInt8),
    c2 Tuple(x Array(Tuple(`size0` String)), y UInt8),
    c3 Tuple(x Array(Tuple(`size0` LowCardinality(String))), y UInt8),
    c4 Tuple(x Array(Tuple(`size0` Array(UInt64))), y UInt8),
    c5 Tuple(x Array(Tuple(`size0` Nullable(UInt64))), y UInt8),
    c6 Tuple(x Array(Array(Tuple(`size0` UInt64))), y UInt8),
    c7 Tuple(x Array(Array(Tuple(`size1` UInt64))), y UInt8),
    c8 Tuple(x Array(Nullable(Tuple(`size0` Int64))), y UInt8),
    c9 Tuple(x Map(String, Array(Tuple(`size1` UInt64))), y UInt8),
    -- A Tuple element named like an automatic subcolumn of a sibling element.
    c10 Tuple(x Tuple(`a` String, `a.size` UInt64), y UInt8),
    c11 Tuple(x Tuple(`a` Array(UInt64), `a.size0` UInt64), y UInt8),
    c12 Tuple(x Tuple(`a` Nullable(UInt64), `a.null` UInt8), y UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

CREATE TABLE t_cache_compact
(
    c1 Tuple(x Array(Tuple(`size0` UInt64)), y UInt8),
    c2 Tuple(x Array(Tuple(`size0` String)), y UInt8),
    c3 Tuple(x Array(Tuple(`size0` LowCardinality(String))), y UInt8),
    c4 Tuple(x Array(Tuple(`size0` Array(UInt64))), y UInt8),
    c5 Tuple(x Array(Tuple(`size0` Nullable(UInt64))), y UInt8),
    c6 Tuple(x Array(Array(Tuple(`size0` UInt64))), y UInt8),
    c7 Tuple(x Array(Array(Tuple(`size1` UInt64))), y UInt8),
    c8 Tuple(x Array(Nullable(Tuple(`size0` Int64))), y UInt8),
    c9 Tuple(x Map(String, Array(Tuple(`size1` UInt64))), y UInt8),
    c10 Tuple(x Tuple(`a` String, `a.size` UInt64), y UInt8),
    c11 Tuple(x Tuple(`a` Array(UInt64), `a.size0` UInt64), y UInt8),
    c12 Tuple(x Tuple(`a` Nullable(UInt64), `a.null` UInt8), y UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_cache_wide SELECT ([tuple(100), tuple(200)], 1), ([tuple('abc')], 2), ([tuple('abc')], 3), ([tuple([10, 20])], 4), ([tuple(1), tuple(NULL)], 5), ([[tuple(10), tuple(20)]], 6), ([[tuple(10), tuple(20)]], 7), ([tuple(1), NULL], 8), (map('k', [tuple(1), tuple(2)]), 9), (('abc', 99), 10), (([10, 20], 99), 11), ((5, 7), 12);
INSERT INTO t_cache_compact SELECT ([tuple(100), tuple(200)], 1), ([tuple('abc')], 2), ([tuple('abc')], 3), ([tuple([10, 20])], 4), ([tuple(1), tuple(NULL)], 5), ([[tuple(10), tuple(20)]], 6), ([[tuple(10), tuple(20)]], 7), ([tuple(1), NULL], 8), (map('k', [tuple(1), tuple(2)]), 9), (('abc', 99), 10), (([10, 20], 99), 11), ((5, 7), 12);

SELECT * FROM t_cache_wide FORMAT Vertical;
SELECT c1.x, c2.x, c3.x, c4.x, c5.x, c6.x, c7.x, c8.x, c9.x, c10.x, c11.x, c12.x FROM t_cache_wide FORMAT Vertical;
SELECT * FROM t_cache_compact FORMAT Vertical;
SELECT c1.x, c2.x, c3.x, c4.x, c5.x, c6.x, c7.x, c8.x, c9.x, c10.x, c11.x, c12.x FROM t_cache_compact FORMAT Vertical;

DROP TABLE t_cache_wide;
DROP TABLE t_cache_compact;

-- The same with JSON typed paths named like a substream of a sibling path or like an internal stream.
DROP TABLE IF EXISTS t_cache_json_wide;
DROP TABLE IF EXISTS t_cache_json_compact;

CREATE TABLE t_cache_json_wide
(
    c1 Tuple(x JSON(`object_shared_data.0.size0` Int64), y UInt8),
    c2 Tuple(x JSON(`a` Array(Int64), `a.size0` Int64), y UInt8),
    c3 Tuple(x JSON(`a` Nullable(Int64), `a.null` Int64), y UInt8),
    c4 Tuple(x JSON(`a` String, `a.size` Int64), y UInt8),
    c5 Tuple(x JSON(`a` Dynamic, `a.Int64` Int64), y UInt8),
    c6 Tuple(x JSON(`a` Variant(Int64, String), `a.Int64` Int64), y UInt8),
    c7 Tuple(x JSON(`a` JSON(`b` Int64), `a.b` Int64), y UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

CREATE TABLE t_cache_json_compact
(
    c1 Tuple(x JSON(`object_shared_data.0.size0` Int64), y UInt8),
    c2 Tuple(x JSON(`a` Array(Int64), `a.size0` Int64), y UInt8),
    c3 Tuple(x JSON(`a` Nullable(Int64), `a.null` Int64), y UInt8),
    c4 Tuple(x JSON(`a` String, `a.size` Int64), y UInt8),
    c5 Tuple(x JSON(`a` Dynamic, `a.Int64` Int64), y UInt8),
    c6 Tuple(x JSON(`a` Variant(Int64, String), `a.Int64` Int64), y UInt8),
    c7 Tuple(x JSON(`a` JSON(`b` Int64), `a.b` Int64), y UInt8)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

INSERT INTO t_cache_json_wide VALUES (('{"object_shared_data.0.size0" : 1}', 1), ('{"a" : [1, 2], "a.size0" : 7}', 2), ('{"a" : 1, "a.null" : 7}', 3), ('{"a" : "abc", "a.size" : 7}', 4), ('{"a" : 1, "a.Int64" : 7}', 5), ('{"a" : 1, "a.Int64" : 7}', 6), ('{"a" : {"b" : 1}}', 7));
INSERT INTO t_cache_json_compact VALUES (('{"object_shared_data.0.size0" : 1}', 1), ('{"a" : [1, 2], "a.size0" : 7}', 2), ('{"a" : 1, "a.null" : 7}', 3), ('{"a" : "abc", "a.size" : 7}', 4), ('{"a" : 1, "a.Int64" : 7}', 5), ('{"a" : 1, "a.Int64" : 7}', 6), ('{"a" : {"b" : 1}}', 7));

SELECT * FROM t_cache_json_wide FORMAT Vertical;
SELECT c1.x, c2.x, c3.x, c4.x, c5.x, c6.x, c7.x FROM t_cache_json_wide FORMAT Vertical;
SELECT * FROM t_cache_json_compact FORMAT Vertical;
SELECT c1.x, c2.x, c3.x, c4.x, c5.x, c6.x, c7.x FROM t_cache_json_compact FORMAT Vertical;

DROP TABLE t_cache_json_wide;
DROP TABLE t_cache_json_compact;

-- Whole column and subcolumn reads must keep the same key: the wide reader keys streams by file name, so
-- without sharing the second read continues from where the first stopped. Needs several granules.
DROP TABLE IF EXISTS t_cache_sharing;

SET flatten_nested = 0;

CREATE TABLE t_cache_sharing
(
    i UInt64,
    arr Array(Nullable(UInt64)),
    arr2 Array(Array(String)),
    m Map(String, UInt64),
    n Nested(a UInt64, b String),
    s Nullable(String),
    j JSON(`p` Int64)
)
ENGINE = MergeTree ORDER BY i SETTINGS min_bytes_for_wide_part = 0, index_granularity = 3;

INSERT INTO t_cache_sharing SELECT number, range(number % 4), [[toString(number)]], map('k', number), [(number, toString(number))], toString(number), '{"p" : ' || toString(number) || '}' FROM numbers(20);

SELECT sum(cityHash64(arr)), sum(arr.size0), sum(cityHash64(arr.null)) FROM t_cache_sharing;
SELECT sum(cityHash64(arr2)), sum(arr2.size0), sum(cityHash64(arr2.size1)) FROM t_cache_sharing;
SELECT sum(cityHash64(m)), sum(m.size0), sum(cityHash64(m.keys)), sum(cityHash64(m.values)) FROM t_cache_sharing;
SELECT sum(cityHash64(n)), sum(n.size0), sum(cityHash64(n.a)) FROM t_cache_sharing;
SELECT sum(cityHash64(s)), sum(s.null), sum(cityHash64(s.size)) FROM t_cache_sharing;
SELECT sum(cityHash64(j)), sum(j.p) FROM t_cache_sharing;

DROP TABLE t_cache_sharing;

-- Sparse elements are a separate substream, which the key must keep distinct from the column itself.
DROP TABLE IF EXISTS t_cache_sparse;

CREATE TABLE t_cache_sparse (i UInt64, s String, arr Array(UInt64), t Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1, index_granularity = 3;

INSERT INTO t_cache_sparse SELECT number, if(number % 10 = 0, 'x', ''), if(number % 10 = 0, [number], []), (if(number % 10 = 0, number, 0), '') FROM numbers(20);

SELECT sum(serialization_kind = 'Sparse') > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_cache_sparse' AND active;
SELECT sum(cityHash64(s)), sum(s.size), sum(cityHash64(arr)), sum(arr.size0), sum(cityHash64(t)), sum(t.a) FROM t_cache_sparse;

DROP TABLE t_cache_sparse;

-- StorageLog and StorageTinyLog use the same cache.
DROP TABLE IF EXISTS t_cache_log;
DROP TABLE IF EXISTS t_cache_tiny_log;

CREATE TABLE t_cache_log (c Array(Tuple(`size0` UInt64)), d Tuple(`a` String, `a.size` UInt64)) ENGINE = Log;
CREATE TABLE t_cache_tiny_log (c Array(Tuple(`size0` UInt64)), d Tuple(`a` String, `a.size` UInt64)) ENGINE = TinyLog;
INSERT INTO t_cache_log VALUES ([(100), (200)], ('abc', 99)), ([(300)], ('de', 98));
INSERT INTO t_cache_tiny_log VALUES ([(100), (200)], ('abc', 99)), ([(300)], ('de', 98));
SELECT c, d FROM t_cache_log ORDER BY c;
SELECT c, d FROM t_cache_tiny_log ORDER BY c;

DROP TABLE t_cache_log;
DROP TABLE t_cache_tiny_log;
