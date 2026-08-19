-- Tags: no-old-analyzer

-- `allow_inequality_join_as_cross_join` routes a keyless outer `JOIN ON` condition to the hash join
-- over a constant join key instead of to the block nested loop join, which is how such a join was
-- executed before that operator existed. The two must answer the same.

SET join_algorithm = 'direct,parallel_hash,hash';
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS ck_l;
DROP TABLE IF EXISTS ck_r;

CREATE TABLE ck_l (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ck_r (id Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO ck_l SELECT number, number FROM numbers(6);
INSERT INTO ck_r SELECT number, 3 - number FROM numbers(4);

-- The plan: a `Join` step with the setting on, a `BlockNestedLoopJoin` step with it off.
SELECT 'plan hash', count() FROM (EXPLAIN SELECT count() FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
    SETTINGS allow_inequality_join_as_cross_join = 1) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'plan operator', count() FROM (EXPLAIN SELECT count() FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
    SETTINGS allow_inequality_join_as_cross_join = 0) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- The same result either way, for every kind the constant key covers and for both paddings of an
-- unmatched row.
SELECT kind, use_nulls, hash_join = block_nested_loop_join AS same, hash_join FROM
(
    SELECT 'left' AS kind, 0 AS use_nulls,
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 1, join_use_nulls = 0) AS hash_join,
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 0, join_use_nulls = 0) AS block_nested_loop_join
    UNION ALL
    SELECT 'left', 1,
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 1, join_use_nulls = 1),
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l LEFT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 0, join_use_nulls = 1)
    UNION ALL
    SELECT 'right', 0,
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l RIGHT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 1, join_use_nulls = 0),
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l RIGHT JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 0, join_use_nulls = 0)
    UNION ALL
    SELECT 'full', 0,
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l FULL JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 1, join_use_nulls = 0),
        (SELECT sum(cityHash64(l.id, l.x, r.id, r.y)) FROM ck_l l FULL JOIN ck_r r ON l.x < r.y
            SETTINGS allow_inequality_join_as_cross_join = 0, join_use_nulls = 0)
)
ORDER BY kind, use_nulls;

-- An `ALL INNER` join goes to the hash join as well, with the condition filtered above it instead of
-- evaluated inside, and answers the same as the cross join rewrite it replaces.
-- The exact algorithm name depends on settings the test harness randomizes (`parallel_hash`, the
-- spilling wrapper), so only the family it belongs to is asserted.
SELECT 'inner algorithm', countIf(explain LIKE '%HashJoin%'), countIf(explain LIKE '%ConstantJoin%') FROM
    (EXPLAIN actions = 1 SELECT count() FROM ck_l l INNER JOIN ck_r r ON l.x < r.y
        SETTINGS allow_inequality_join_as_cross_join = 1)
    WHERE explain LIKE '%Algorithm: %';
SELECT 'inner cross algorithm', countIf(explain LIKE '%HashJoin%'), countIf(explain LIKE '%ConstantJoin%') FROM
    (EXPLAIN actions = 1 SELECT count() FROM ck_l l INNER JOIN ck_r r ON l.x < r.y
        SETTINGS allow_inequality_join_as_cross_join = 0)
    WHERE explain LIKE '%Algorithm: %';
SELECT 'inner', count() FROM ck_l l INNER JOIN ck_r r ON l.x < r.y SETTINGS allow_inequality_join_as_cross_join = 1;
SELECT 'inner', count() FROM ck_l l INNER JOIN ck_r r ON l.x < r.y SETTINGS allow_inequality_join_as_cross_join = 0;

-- A strictness the hash join cannot execute without a key is still rejected when the operator is off.
SELECT count() FROM ck_l l LEFT ANY JOIN ck_r r ON l.x < r.y
    SETTINGS allow_inequality_join_as_cross_join = 1, allow_block_nested_loop_join = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE ck_l;
DROP TABLE ck_r;
