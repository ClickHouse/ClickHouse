-- ConstantJoin (CROSS, comma and constant-predicate joins) emits every left row in probe order,
-- so it opts in to IJoin::preservesLeftBlockOrder(). The default is fail-closed, so without that
-- opt-in read-in-order-through-join stops propagating and aggregation-in-order, DISTINCT-in-order,
-- LIMIT BY and the ordered read all silently fall back to an unordered read. Issue #109216.

DROP TABLE IF EXISTS cj_l_04500;
DROP TABLE IF EXISTS cj_r_04500;

CREATE TABLE cj_l_04500 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE cj_r_04500 (m UInt32) ENGINE = MergeTree ORDER BY m;
INSERT INTO cj_l_04500 SELECT number % 50, number FROM numbers(4000);
INSERT INTO cj_r_04500 VALUES (1);

SET explain_query_plan_default = 'legacy';

-- Pin the whole read-in-order trio: the stateless runner randomizes all three, and with
-- query_plan_read_in_order_through_join = 0 findReadingStep never traverses the join, so the
-- InOrder assertion below would read 0 no matter what preservesLeftBlockOrder() returns.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;

-- The left ReadFromMergeTree under a constant-predicate join MUST read in order. Asserting an
-- exact positive count, so dropping the opt-in (which yields ReadType: Default) fails this line.
SELECT countIf(explain LIKE '%InOrder%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT l.k, count() FROM cj_l_04500 AS l LEFT JOIN cj_r_04500 AS r ON 1 = 1
    GROUP BY l.k ORDER BY l.k
    SETTINGS optimize_aggregation_in_order = 1, max_threads = 1,
             query_plan_join_swap_table = 'false', enable_parallel_replicas = 0,
             optimize_read_in_order = 1, query_plan_read_in_order = 1,
             query_plan_read_in_order_through_join = 1
) WHERE explain LIKE '%ReadType%';

-- Result oracle: the order the optimization is allowed to rely on must actually hold, otherwise
-- the opt-in would trade a wrong result for the speedup. uniqExact(l.k) within each GROUP BY l.k
-- is 1 exactly when no row was mis-grouped, so max and min must both be 1 over all 50 groups.
SELECT max(u), min(u), count() FROM (
    SELECT l.k, uniqExact(l.k) AS u FROM cj_l_04500 AS l LEFT JOIN cj_r_04500 AS r ON 1 = 1
    GROUP BY l.k
) SETTINGS optimize_aggregation_in_order = 1, max_threads = 1,
           query_plan_join_swap_table = 'false', enable_parallel_replicas = 0;

-- Same for an explicit CROSS JOIN, and for the other two consumers of the contract.
SELECT count() FROM (
    SELECT DISTINCT l.k FROM cj_l_04500 AS l CROSS JOIN cj_r_04500 AS r
    SETTINGS optimize_distinct_in_order = 1, max_threads = 1, enable_parallel_replicas = 0
);

SELECT count() FROM (
    SELECT l.k FROM cj_l_04500 AS l LEFT JOIN cj_r_04500 AS r ON 1 = 1
    ORDER BY l.k LIMIT 1 BY l.k
    SETTINGS max_threads = 1, enable_parallel_replicas = 0
);

-- A plain top-level ORDER BY over the join must be truly sorted: the sort may only be elided
-- because the join really does hand the left rows over in order.
SELECT groupArray(k) = arraySort(groupArray(k)) FROM (
    SELECT l.k AS k FROM cj_l_04500 AS l LEFT JOIN cj_r_04500 AS r ON 1 = 1 ORDER BY l.k
) SETTINGS max_threads = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1,
           query_plan_read_in_order_through_join = 1, query_plan_join_swap_table = 'false',
           enable_parallel_replicas = 0;

DROP TABLE cj_l_04500;
DROP TABLE cj_r_04500;
