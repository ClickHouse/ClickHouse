-- An arrayJoin in the projection changes row multiplicity after the
-- aggregation (a group can expand to zero rows), so the top-K optimization
-- must not fire: pruning to the smallest N groups would lose groups the
-- LIMIT still needs.  Regression: `range(k % 2)` annihilates even keys, and
-- with the heap active the query returned 2 rows instead of 5.

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;
-- Keep the no-ORDER-BY shape reachable for the plan pass (the trivial analyzer
-- pass otherwise handles it via max_rows_to_group_by).
SET optimize_trivial_group_by_limit_query = 0;

DROP TABLE IF EXISTS t_gbytopk_ajp;

CREATE TABLE t_gbytopk_ajp (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_gbytopk_ajp SELECT number FROM numbers(100000);

SELECT 'arrayJoin in projection, with ORDER BY: correct rows';
SELECT k, arrayJoin(range(k % 2)) AS a
FROM t_gbytopk_ajp
GROUP BY k
ORDER BY k ASC
LIMIT 5;

SELECT 'arrayJoin in projection, no ORDER BY: correct row count';
SELECT count() FROM
(
    SELECT k, arrayJoin(range(k % 2)) AS a
    FROM t_gbytopk_ajp
    GROUP BY k
    LIMIT 5
);

SELECT 'no Top-K in EXPLAIN, with ORDER BY';
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT k, arrayJoin(range(k % 2)) AS a
    FROM t_gbytopk_ajp
    GROUP BY k
    ORDER BY k ASC
    LIMIT 5
)
WHERE explain LIKE '%Top-K%';

SELECT 'no Top-K in EXPLAIN, no ORDER BY';
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT k, arrayJoin(range(k % 2)) AS a
    FROM t_gbytopk_ajp
    GROUP BY k
    LIMIT 5
)
WHERE explain LIKE '%Top-K%';

DROP TABLE t_gbytopk_ajp;
