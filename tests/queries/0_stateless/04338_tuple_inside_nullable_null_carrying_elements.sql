SET enable_analyzer = 1;

-- { echo }
-- Elements of a `Nullable(Tuple(...))` that can represent NULL themselves without being wrapped into
-- Nullable: Dynamic, Variant and LowCardinality(Nullable(...)). Rows where the outer tuple is NULL must
-- read as NULL through such subcolumns.

SET allow_experimental_nullable_tuple_type = 1;

-- Expression path: the ternary operator evaluates both branches, so the extracted subcolumns contain
-- non-NULL leftovers in rows where the outer tuple is NULL.
SELECT if(number % 2, NULL, tuple(number::Dynamic)::Tuple(a Dynamic)) AS t, t.a, isNull(t.a) FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT if(number % 2, NULL, tuple(number)::Tuple(a Variant(UInt64))) AS t, t.a, isNull(t.a) FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT if(number % 2, NULL, tuple(toString(number))::Tuple(s LowCardinality(Nullable(String)))) AS t, t.s, isNull(t.s) FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'disable';

-- Wide part, one row per granule.
DROP TABLE IF EXISTS t_null_carrying_wide;
CREATE TABLE t_null_carrying_wide (tup Nullable(Tuple(a Dynamic, v Variant(UInt64), s LowCardinality(Nullable(String)))))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_null_carrying_wide SELECT number % 2 ? NULL : tuple(number::Dynamic, number, toString(number)) FROM numbers(4);
SELECT tup.a, tup.v, tup.s, isNull(tup.a), isNull(tup.v), isNull(tup.s) FROM t_null_carrying_wide;
SELECT tup, tup.a, tup.v, tup.s FROM t_null_carrying_wide;
SELECT count(tup.a), count(tup.v), count(tup.s) FROM t_null_carrying_wide;
-- Element subcolumns of Variant and Dynamic are Nullable built from the discriminators; the outer null
-- map must be combined into their null maps as well.
SELECT tup.a.UInt64, tup.v.UInt64, isNull(tup.a.UInt64), isNull(tup.v.UInt64) FROM t_null_carrying_wide;
DROP TABLE t_null_carrying_wide;

-- Compact part.
DROP TABLE IF EXISTS t_null_carrying_compact;
CREATE TABLE t_null_carrying_compact (tup Nullable(Tuple(a Dynamic, v Variant(UInt64), s LowCardinality(Nullable(String)))))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_null_carrying_compact SELECT number % 2 ? NULL : tuple(number::Dynamic, number, toString(number)) FROM numbers(4);
SELECT tup.a, tup.v, tup.s, isNull(tup.a), isNull(tup.v), isNull(tup.s) FROM t_null_carrying_compact;
SELECT tup, tup.a, tup.v, tup.s FROM t_null_carrying_compact;
SELECT count(tup.a), count(tup.v), count(tup.s) FROM t_null_carrying_compact;
SELECT tup.a.UInt64, tup.v.UInt64, isNull(tup.a.UInt64), isNull(tup.v.UInt64) FROM t_null_carrying_compact;
DROP TABLE t_null_carrying_compact;

-- Memory engine stores the in-memory blocks verbatim, including the non-NULL leftovers of the ternary
-- operator in rows where the outer tuple is NULL.
DROP TABLE IF EXISTS t_null_carrying_mem;
CREATE TABLE t_null_carrying_mem (tup Nullable(Tuple(a Dynamic, v Variant(UInt64), s LowCardinality(Nullable(String))))) ENGINE = Memory;
INSERT INTO t_null_carrying_mem SELECT number % 2 ? NULL : tuple(number::Dynamic, number, toString(number)) FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'disable';
SELECT tup.a, tup.v, tup.s, isNull(tup.a), isNull(tup.v), isNull(tup.s) FROM t_null_carrying_mem;
SELECT count(tup.a), count(tup.v), count(tup.s) FROM t_null_carrying_mem;
DROP TABLE t_null_carrying_mem;

-- A plain LowCardinality(T) element cannot be wrapped in Nullable, so the extracted subcolumn is promoted
-- to LowCardinality(Nullable(T)) and rows where the outer tuple is NULL read as NULL through it.
DROP TABLE IF EXISTS t_lc_plain;
CREATE TABLE t_lc_plain (tup Nullable(Tuple(p LowCardinality(String)))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;
INSERT INTO t_lc_plain VALUES (('a')), (NULL), (('b')), (NULL);
SELECT toTypeName(tup.p), tup, tup.p, isNull(tup) FROM t_lc_plain;
DROP TABLE t_lc_plain;
