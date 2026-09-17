-- Tags: no-fasttest
-- no-fasttest: COLLATE needs ICU
-- The hint is not serialized, so a serialized plan never runs the prefilter and the control below
-- would read 0 (the Stress job passes serialize_query_plan=1 to the client).
SET serialize_query_plan = 0;
-- Randomized over [0,1,10,100,1000,100000]; the pass declines a bound above it.
SET query_plan_max_limit_for_top_k_optimization = 100000;

DROP TABLE IF EXISTS t_wtkp_collate;
CREATE TABLE t_wtkp_collate (p UInt8, o UInt8) ENGINE = Memory;
INSERT INTO t_wtkp_collate VALUES (1,10),(1,9),(1,8),(1,8),(1,8),(1,7),(2,10),(2,9),(2,7),(2,7);

SELECT '15 collation', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY toString(o) DESC COLLATE 'en') AS rk FROM t_wtkp_collate) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
-- The control differs from the query above only in the COLLATE clause, so a 1 here is what makes the
-- 0 above a statement about the collator instead of about this query shape.
SELECT '15 control, same query without COLLATE', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY toString(o) DESC) AS rk FROM t_wtkp_collate) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';

DROP TABLE t_wtkp_collate;
