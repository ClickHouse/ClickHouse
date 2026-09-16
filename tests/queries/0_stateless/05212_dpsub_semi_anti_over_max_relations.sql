-- Tags: no-random-settings
-- no-random-settings: the randomizer varies the join order settings this test pins.

-- DPsub plans at most `DPSUB_MAX_RELATIONS` (12) tables, and it is the only algorithm that can plan
-- a group of tables holding a semi/anti join. So the semi/anti joins only join that group while it
-- stays within the limit. Past it they are left out, and the group -- now an ordinary one -- is
-- handed to the next algorithm in `query_plan_optimize_join_order_algorithm` whole, rather than
-- being cut into pieces small enough for DPsub. Results must not depend on any of that.

DROP TABLE IF EXISTS t_05212_l;
DROP TABLE IF EXISTS t_05212_semi;
DROP TABLE IF EXISTS t_05212_anti;

CREATE TABLE t_05212_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05212_semi (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05212_anti (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_05212_l SELECT number FROM numbers(300);      -- 0..299
INSERT INTO t_05212_semi SELECT number FROM numbers(200);   -- 0..199, keeps 0..199
INSERT INTO t_05212_anti SELECT 150 + number FROM numbers(150); -- 150..299, drops 150..199

SELECT 'within the limit: 6 tables, semi/anti are reordered by DPsub';

-- 4 inner + semi + anti = 6 tables, so DPsub takes the whole group.
SELECT count()
FROM t_05212_l AS a
INNER JOIN t_05212_l AS b ON a.k = b.k
INNER JOIN t_05212_l AS c ON a.k = c.k
INNER JOIN t_05212_l AS d ON a.k = d.k
LEFT SEMI JOIN t_05212_semi AS s ON a.k = s.k
LEFT ANTI JOIN t_05212_anti AS n ON a.k = n.k;

SELECT 'over the limit: 16 tables, semi/anti stay out and greedy takes the group';

-- 14 inner + semi + anti = 16 tables. Raising the limit past DPsub's own bound is what makes the
-- fallback observable; the same answer must come out.
SELECT count()
FROM t_05212_l AS a
INNER JOIN t_05212_l AS t1 ON a.k = t1.k
INNER JOIN t_05212_l AS t2 ON a.k = t2.k
INNER JOIN t_05212_l AS t3 ON a.k = t3.k
INNER JOIN t_05212_l AS t4 ON a.k = t4.k
INNER JOIN t_05212_l AS t5 ON a.k = t5.k
INNER JOIN t_05212_l AS t6 ON a.k = t6.k
INNER JOIN t_05212_l AS t7 ON a.k = t7.k
INNER JOIN t_05212_l AS t8 ON a.k = t8.k
INNER JOIN t_05212_l AS t9 ON a.k = t9.k
INNER JOIN t_05212_l AS t10 ON a.k = t10.k
INNER JOIN t_05212_l AS t11 ON a.k = t11.k
INNER JOIN t_05212_l AS t12 ON a.k = t12.k
INNER JOIN t_05212_l AS t13 ON a.k = t13.k
LEFT SEMI JOIN t_05212_semi AS s ON a.k = s.k
LEFT ANTI JOIN t_05212_anti AS n ON a.k = n.k
SETTINGS query_plan_optimize_join_order_limit = 25;

SELECT 'the fallback chain still answers the same without DPsub';

SELECT count()
FROM t_05212_l AS a
INNER JOIN t_05212_l AS t1 ON a.k = t1.k
INNER JOIN t_05212_l AS t2 ON a.k = t2.k
INNER JOIN t_05212_l AS t3 ON a.k = t3.k
INNER JOIN t_05212_l AS t4 ON a.k = t4.k
INNER JOIN t_05212_l AS t5 ON a.k = t5.k
INNER JOIN t_05212_l AS t6 ON a.k = t6.k
INNER JOIN t_05212_l AS t7 ON a.k = t7.k
INNER JOIN t_05212_l AS t8 ON a.k = t8.k
INNER JOIN t_05212_l AS t9 ON a.k = t9.k
INNER JOIN t_05212_l AS t10 ON a.k = t10.k
INNER JOIN t_05212_l AS t11 ON a.k = t11.k
INNER JOIN t_05212_l AS t12 ON a.k = t12.k
INNER JOIN t_05212_l AS t13 ON a.k = t13.k
LEFT SEMI JOIN t_05212_semi AS s ON a.k = s.k
LEFT ANTI JOIN t_05212_anti AS n ON a.k = n.k
SETTINGS query_plan_optimize_join_order_limit = 25, query_plan_optimize_join_order_algorithm = 'greedy';

SELECT 'a 15 table group reaches the next algorithm whole, not cut to DPsub size';

-- DPsub turns a 15 table group down, so greedy plans it. If the group were first cut into pieces
-- DPsub accepts, greedy would only ever see the pieces and would pick a different order than it
-- does on its own. Identical plans are what says the group arrived whole.
SELECT
(
    SELECT groupArray(explain) FROM (
        EXPLAIN PLAN
        SELECT count()
        FROM t_05212_l AS a
        INNER JOIN t_05212_l AS t1 ON a.k = t1.k
        INNER JOIN t_05212_l AS t2 ON a.k = t2.k
        INNER JOIN t_05212_l AS t3 ON a.k = t3.k
        INNER JOIN t_05212_l AS t4 ON a.k = t4.k
        INNER JOIN t_05212_l AS t5 ON a.k = t5.k
        INNER JOIN t_05212_l AS t6 ON a.k = t6.k
        INNER JOIN t_05212_l AS t7 ON a.k = t7.k
        INNER JOIN t_05212_l AS t8 ON a.k = t8.k
        INNER JOIN t_05212_l AS t9 ON a.k = t9.k
        INNER JOIN t_05212_l AS t10 ON a.k = t10.k
        INNER JOIN t_05212_l AS t11 ON a.k = t11.k
        INNER JOIN t_05212_l AS t12 ON a.k = t12.k
        INNER JOIN t_05212_l AS t13 ON a.k = t13.k
        SETTINGS query_plan_optimize_join_order_limit = 25,
                 query_plan_optimize_join_order_algorithm = 'dpsub,greedy'
    )
) =
(
    SELECT groupArray(explain) FROM (
        EXPLAIN PLAN
        SELECT count()
        FROM t_05212_l AS a
        INNER JOIN t_05212_l AS t1 ON a.k = t1.k
        INNER JOIN t_05212_l AS t2 ON a.k = t2.k
        INNER JOIN t_05212_l AS t3 ON a.k = t3.k
        INNER JOIN t_05212_l AS t4 ON a.k = t4.k
        INNER JOIN t_05212_l AS t5 ON a.k = t5.k
        INNER JOIN t_05212_l AS t6 ON a.k = t6.k
        INNER JOIN t_05212_l AS t7 ON a.k = t7.k
        INNER JOIN t_05212_l AS t8 ON a.k = t8.k
        INNER JOIN t_05212_l AS t9 ON a.k = t9.k
        INNER JOIN t_05212_l AS t10 ON a.k = t10.k
        INNER JOIN t_05212_l AS t11 ON a.k = t11.k
        INNER JOIN t_05212_l AS t12 ON a.k = t12.k
        INNER JOIN t_05212_l AS t13 ON a.k = t13.k
        SETTINGS query_plan_optimize_join_order_limit = 25,
                 query_plan_optimize_join_order_algorithm = 'greedy'
    )
);

DROP TABLE t_05212_l;
DROP TABLE t_05212_semi;
DROP TABLE t_05212_anti;
