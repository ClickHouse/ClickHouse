-- Correlated EXISTS / NOT EXISTS decorrelates into an ANY (RIGHT) JOIN. The plan optimization
-- query_plan_convert_any_join_to_semi_or_anti_join rewrites it into SEMI / ANTI. That rewrite
-- must only happen when an enabled join_algorithm can execute the converted join: full_sorting_merge
-- does not implement SEMI/ANTI, so before this fix the pass turned an executable full_sorting_merge
-- query into a NOT_IMPLEMENTED error. The pass must instead keep the ANY join. Regression for #111075.

SET enable_analyzer = 1;
-- Pin the optimization under test: CI randomizes it (tests/clickhouse-test), and if it were
-- disabled the queries below would pass trivially without exercising the capability guard.
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t SELECT number, number % 7 FROM numbers(100);

-- full_sorting_merge CAN execute the ANY join but not SEMI/ANTI. Before the fix the pass rewrote
-- to SEMI/ANTI and the query threw NOT_IMPLEMENTED; now it keeps the ANY join and returns the result.
SELECT count() FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count() FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge';

-- The pass is result-preserving, so disabling it gives the same result: full_sorting_merge keeps the ANY join either way.
SELECT count() FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;
SELECT count() FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- Hash-family algorithms execute SEMI/ANTI, so the conversion is still applied and the results are unchanged.
SELECT count() FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'hash';
SELECT count() FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'hash';
SELECT count() FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'auto';
SELECT count() FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'auto';

-- When full_sorting_merge is combined with hash, hash can execute the converted join, so the pass converts.
SELECT count() FROM t AS o WHERE EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count() FROM t AS o WHERE NOT EXISTS (SELECT 1 FROM t AS i WHERE i.b = o.b AND a = 5) SETTINGS join_algorithm = 'full_sorting_merge,hash';

DROP TABLE t;
