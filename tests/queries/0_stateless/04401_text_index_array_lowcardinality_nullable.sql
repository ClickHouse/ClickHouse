-- Text index on Array(LowCardinality(Nullable(String))) must skip NULL array elements during
-- index construction, exactly as for Array(Nullable(String)). Regression test for issue #110039:
-- the array branch of the index aggregator checked isNullable(), which is false for
-- LowCardinality(Nullable), so getDataAt() was called on a NULL element and threw NOT_IMPLEMENTED
-- on INSERT / MATERIALIZE INDEX / merge.

SET mutations_sync = 2;

-- Control: Array(Nullable(String)) already works.
DROP TABLE IF EXISTS t_arr_nullable;
CREATE TABLE t_arr_nullable
(
    id UInt64,
    a Array(Nullable(String)),
    INDEX a_text a TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arr_nullable VALUES (1, [NULL]);
SELECT 'array_nullable', count() FROM t_arr_nullable;

-- The fix: Array(LowCardinality(Nullable(String))) with a NULL element.
DROP TABLE IF EXISTS t_arr_lc_nullable;
CREATE TABLE t_arr_lc_nullable
(
    id UInt64,
    a Array(LowCardinality(Nullable(String))),
    INDEX a_text a TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arr_lc_nullable VALUES (1, [NULL]);
INSERT INTO t_arr_lc_nullable VALUES (2, ['hello', NULL, 'world']);
SELECT 'array_lc_nullable', count() FROM t_arr_lc_nullable;
SELECT 'has_hello', id FROM t_arr_lc_nullable WHERE has(a, 'hello') ORDER BY id;
-- Merge/OPTIMIZE rebuilds the index; it must not wedge on the NULL element.
OPTIMIZE TABLE t_arr_lc_nullable FINAL;
SELECT 'after_optimize', count() FROM t_arr_lc_nullable;

-- Nested stores its field as Array(LowCardinality(Nullable(String))) (fiddle case).
DROP TABLE IF EXISTS t_nested_lc_nullable;
CREATE TABLE t_nested_lc_nullable
(
    id UInt64,
    n Nested(a LowCardinality(Nullable(String))),
    INDEX a_text n.a TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested_lc_nullable VALUES (1, [NULL]);
SELECT 'nested_lc_nullable', count() FROM t_nested_lc_nullable;

-- MATERIALIZE INDEX on pre-existing data must also skip NULL elements.
DROP TABLE IF EXISTS t_materialize_lc_nullable;
CREATE TABLE t_materialize_lc_nullable
(
    id UInt64,
    a Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialize_lc_nullable VALUES (1, ['x', NULL, 'y']);
ALTER TABLE t_materialize_lc_nullable ADD INDEX a_text a TYPE text(tokenizer = 'array') GRANULARITY 1;
ALTER TABLE t_materialize_lc_nullable MATERIALIZE INDEX a_text;
SELECT 'materialize_lc_nullable', id FROM t_materialize_lc_nullable WHERE has(a, 'x') ORDER BY id;

-- Postprocessor variant: when a postprocessor is present, update() routes through
-- tokenizeToArray() whose array branch also checked isNullable() (false for
-- LowCardinality(Nullable)) and threw NOT_IMPLEMENTED on a NULL element. Cover
-- INSERT / MATERIALIZE INDEX / merge for Array(LowCardinality(Nullable(String))).
DROP TABLE IF EXISTS t_arr_lc_nullable_pp;
CREATE TABLE t_arr_lc_nullable_pp
(
    id UInt64,
    a Array(LowCardinality(Nullable(String))),
    INDEX a_text a TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(a)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arr_lc_nullable_pp VALUES (1, [NULL]);
INSERT INTO t_arr_lc_nullable_pp VALUES (2, ['Hello', NULL, 'World']);
SELECT 'pp_array_lc_nullable', count() FROM t_arr_lc_nullable_pp;
SELECT 'pp_has_hello', id FROM t_arr_lc_nullable_pp WHERE hasAnyTokens(a, 'HELLO') ORDER BY id;
OPTIMIZE TABLE t_arr_lc_nullable_pp FINAL;
SELECT 'pp_after_optimize', count() FROM t_arr_lc_nullable_pp;

DROP TABLE IF EXISTS t_materialize_lc_nullable_pp;
CREATE TABLE t_materialize_lc_nullable_pp
(
    id UInt64,
    a Array(LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialize_lc_nullable_pp VALUES (1, ['X', NULL, 'Y']);
ALTER TABLE t_materialize_lc_nullable_pp ADD INDEX a_text a TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(a)) GRANULARITY 1;
ALTER TABLE t_materialize_lc_nullable_pp MATERIALIZE INDEX a_text;
SELECT 'pp_materialize_lc_nullable', id FROM t_materialize_lc_nullable_pp WHERE hasAnyTokens(a, 'x') ORDER BY id;

DROP TABLE t_arr_nullable;
DROP TABLE t_arr_lc_nullable;
DROP TABLE t_nested_lc_nullable;
DROP TABLE t_materialize_lc_nullable;
DROP TABLE t_arr_lc_nullable_pp;
DROP TABLE t_materialize_lc_nullable_pp;
