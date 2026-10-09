-- Random settings limits: query_plan_merge_filter_into_join_condition=(1, None)
-- A `WHERE` equality is NULL for a NULL inside a Tuple and for a NULL Variant, so such a pair is filtered out,
-- even when the equality is also used as a JOIN key, which matches these NULLs as ordinary values.

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS va;
DROP TABLE IF EXISTS vb;
DROP TABLE IF EXISTS da;
DROP TABLE IF EXISTS db;

CREATE TABLE ta (k UInt8, t Tuple(Nullable(UInt64)), n Tuple(UInt64, Tuple(LowCardinality(Nullable(String)))), u Tuple(UInt64)) ENGINE = Memory;
CREATE TABLE tb AS ta;
INSERT INTO ta VALUES (1, tuple(1), (1, tuple('a')), tuple(1)), (2, tuple(NULL), (2, tuple(NULL)), tuple(2)), (3, tuple(2), (3, tuple('b')), tuple(3));
INSERT INTO tb VALUES (1, tuple(1), (1, tuple('a')), tuple(1)), (2, tuple(NULL), (2, tuple(NULL)), tuple(2)), (3, tuple(3), (3, tuple('c')), tuple(4));

-- Row 1 holds a UInt64 on one side and a String on the other: `equals` throws for that pair, the key does not match it.
CREATE TABLE va (k UInt8, v Variant(UInt64, String), w Tuple(Variant(UInt64, String))) ENGINE = Memory;
CREATE TABLE vb AS va;
INSERT INTO va VALUES (1, 1::UInt64, tuple(1::UInt64)), (2, NULL, tuple(NULL)), (3, 'x', tuple('x'));
INSERT INTO vb VALUES (1, 'z', tuple('z')), (2, NULL, tuple(NULL)), (3, 'x', tuple('x'));

CREATE TABLE da (k UInt8, a Int32) ENGINE = Memory;
CREATE TABLE db (k UInt8, d Dynamic) ENGINE = Memory;
INSERT INTO da VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO db VALUES (1, 1), (2, 2), (3, 5);

SELECT '-- Tuple(Nullable(UInt64))';
SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t ORDER BY ALL;
SELECT ta.k, tb.k FROM ta CROSS JOIN tb WHERE ta.t = tb.t ORDER BY ALL;
SELECT ta.k, tb.k FROM ta INNER JOIN tb ON ta.k = tb.k WHERE ta.t = tb.t ORDER BY ALL;
SELECT ta.k, tb.k FROM ta INNER JOIN tb ON ta.k = tb.k WHERE identity(ta.t) = identity(tb.t) ORDER BY ALL;
SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t ORDER BY ALL
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 1;
SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t ORDER BY ALL
SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 0;
SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t ORDER BY ALL SETTINGS cross_to_inner_join_rewrite = 2;

SELECT '-- Tuple(UInt64, Tuple(LowCardinality(Nullable(String))))';
SELECT ta.k, tb.k FROM ta, tb WHERE ta.n = tb.n ORDER BY ALL;
SELECT ta.k, tb.k FROM ta INNER JOIN tb ON ta.k = tb.k WHERE ta.n = tb.n ORDER BY ALL;

SELECT '-- Variant(UInt64, String)';
SELECT va.k, vb.k FROM va, vb WHERE va.v = vb.v ORDER BY ALL;
SELECT va.k, vb.k FROM va INNER JOIN vb ON va.k = vb.k WHERE va.v = vb.v ORDER BY ALL;
SELECT va.k, vb.k FROM va, vb WHERE va.v = vb.v ORDER BY ALL SETTINGS cross_to_inner_join_rewrite = 0;
SELECT k FROM (SELECT va.k AS k, va.v AS a, vb.v AS b FROM va INNER JOIN vb ON va.k = vb.k) WHERE a = b ORDER BY ALL;
SELECT va.k, vb.k FROM va INNER JOIN vb ON va.k = vb.k WHERE va.w = vb.w ORDER BY ALL;
SELECT va.k, vb.k FROM va, vb WHERE identity(tuple(va.v)) = identity(tuple(vb.v)) ORDER BY ALL;

SELECT '-- Dynamic with allow_dynamic_type_in_join_keys = 0: the equality stays a filter, not a JOIN key';
SELECT da.k, db.k FROM da, db WHERE da.a = db.d ORDER BY ALL SETTINGS allow_dynamic_type_in_join_keys = 0;
SELECT da.k, db.k FROM da, db WHERE da.a = db.d AND da.k = db.k ORDER BY ALL SETTINGS allow_dynamic_type_in_join_keys = 0;

SELECT '-- The comma join is still a hash join on the key';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)')
FROM (
    EXPLAIN SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t
    SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 1
);
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)')
FROM (
    EXPLAIN SELECT ta.k, tb.k FROM ta, tb WHERE ta.t = tb.t
    SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 0
);

SELECT '-- The equality is still merged into the JOIN condition';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)')
FROM (
    EXPLAIN SELECT ta.k, tb.k FROM ta INNER JOIN tb ON ta.k = tb.k WHERE ta.t = tb.t
    SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 1
);

SELECT '-- An equality of Tuples without NULLs leaves no filter';
SELECT count()
FROM (
    EXPLAIN SELECT ta.k, tb.k FROM ta INNER JOIN tb ON ta.k = tb.k WHERE ta.u = tb.u
    SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 1
)
WHERE explain ILIKE '%Filter%';

DROP TABLE ta;
DROP TABLE tb;
DROP TABLE va;
DROP TABLE vb;
DROP TABLE da;
DROP TABLE db;
