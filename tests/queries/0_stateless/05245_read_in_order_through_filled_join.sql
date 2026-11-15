-- Tags: no-parallel-replicas
-- Assertions on reading do not hold with parallel replicas.

-- Read-in-order through a FILLED join (`FilledJoinStep`): a Join-engine table and a direct join
-- are filled before the query runs and preserve the left stream's order unconditionally, so the
-- read-in-order-through-join optimization must traverse `FilledJoinStep` too - it never gets the
-- `keepLeftPipelineInOrder` pin, the unconditional guarantee is required.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS fj_left;
DROP TABLE IF EXISTS fj_right;

CREATE TABLE fj_left (k UInt32, j UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO fj_left SELECT number, (number * 37) % 150 FROM numbers(1000);

CREATE TABLE fj_right (j UInt32, v String) ENGINE = Join(ANY, LEFT, j);
INSERT INTO fj_right SELECT number, concat('v', toString(number)) FROM numbers(125);

SELECT '-- The left read under the filled join must be in order';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT fj_left.k, fj_right.v FROM fj_left ANY LEFT JOIN fj_right ON fj_left.j = fj_right.j
    ORDER BY fj_left.k LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT '-- One output row per left row, in the left order';

SELECT count(), countIf(v = ''), arraySort(groupArray(materialize(k))) = groupArray(materialize(k)) FROM (
    SELECT fj_left.k AS k, fj_right.v AS v FROM fj_left ANY LEFT JOIN fj_right ON fj_left.j = fj_right.j
    ORDER BY fj_left.k
    SETTINGS max_threads = 1
);

SELECT fj_left.k, fj_right.v FROM fj_left ANY LEFT JOIN fj_right ON fj_left.j = fj_right.j
ORDER BY fj_left.k LIMIT 3;

DROP TABLE fj_left;
DROP TABLE fj_right;
