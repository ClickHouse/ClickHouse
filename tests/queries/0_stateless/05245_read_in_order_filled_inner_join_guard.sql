-- Tags: no-parallel-replicas

-- The join-specific guards of read-in-order-through-join must see FILLED joins too, not only
-- `JoinStep`: a filled INNER join (`FilledJoinStep` over a Join-engine table) filters the ordered
-- left streams exactly like a plain INNER join. Without the virtual row the merge above the read
-- cannot advance until every stream yields its first surviving row, so the in-order read must be
-- declined. The guard used to track only `JoinStep` instances, and filled INNER joins bypassed it.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS fj_guard_left;
DROP TABLE IF EXISTS fj_guard_inner;
DROP TABLE IF EXISTS fj_guard_left_any;

CREATE TABLE fj_guard_left (k UInt32, j UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO fj_guard_left SELECT number, (number * 37) % 150 FROM numbers(1000);

CREATE TABLE fj_guard_inner (j UInt32, v String) ENGINE = Join(ALL, INNER, j);
INSERT INTO fj_guard_inner SELECT number, concat('v', toString(number)) FROM numbers(125);

SELECT '-- INNER filled join without the virtual row: the in-order read is declined';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT fj_guard_left.k, fj_guard_inner.v FROM fj_guard_left INNER JOIN fj_guard_inner ON fj_guard_left.j = fj_guard_inner.j
    ORDER BY fj_guard_left.k LIMIT 3
    SETTINGS read_in_order_use_virtual_row = 0
) WHERE explain LIKE '%ReadType%';

SELECT '-- INNER filled join with the virtual row: reads in order';

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT fj_guard_left.k, fj_guard_inner.v FROM fj_guard_left INNER JOIN fj_guard_inner ON fj_guard_left.j = fj_guard_inner.j
    ORDER BY fj_guard_left.k LIMIT 3
    SETTINGS read_in_order_use_virtual_row = 1
) WHERE explain LIKE '%ReadType%';

SELECT '-- LEFT filled join does not need the virtual row: reads in order';

CREATE TABLE fj_guard_left_any (j UInt32, v String) ENGINE = Join(ANY, LEFT, j);
INSERT INTO fj_guard_left_any SELECT number, concat('v', toString(number)) FROM numbers(125);

SELECT groupArray(trim(explain)) FROM (
    EXPLAIN PLAN actions = 1
    SELECT fj_guard_left.k, fj_guard_left_any.v FROM fj_guard_left ANY LEFT JOIN fj_guard_left_any ON fj_guard_left.j = fj_guard_left_any.j
    ORDER BY fj_guard_left.k LIMIT 3
    SETTINGS read_in_order_use_virtual_row = 0
) WHERE explain LIKE '%ReadType%';

SELECT '-- The results are correct with and without the virtual row';

SELECT fj_guard_left.k, fj_guard_inner.v FROM fj_guard_left INNER JOIN fj_guard_inner ON fj_guard_left.j = fj_guard_inner.j
ORDER BY fj_guard_left.k LIMIT 3
SETTINGS read_in_order_use_virtual_row = 0;

SELECT fj_guard_left.k, fj_guard_inner.v FROM fj_guard_left INNER JOIN fj_guard_inner ON fj_guard_left.j = fj_guard_inner.j
ORDER BY fj_guard_left.k LIMIT 3
SETTINGS read_in_order_use_virtual_row = 1;

DROP TABLE fj_guard_left;
DROP TABLE fj_guard_inner;
DROP TABLE fj_guard_left_any;
