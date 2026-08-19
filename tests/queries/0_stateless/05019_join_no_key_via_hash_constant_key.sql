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

-- What the constant key does not cover stays where it was: an `ALL INNER` join keeps its
-- cross-join-with-a-filter plan, and a strictness the hash join cannot execute keyless is still
-- rejected when the operator is disabled.
SELECT 'inner cross', count() FROM (EXPLAIN SELECT count() FROM ck_l l INNER JOIN ck_r r ON l.x < r.y
    SETTINGS allow_inequality_join_as_cross_join = 1, cross_to_inner_join_rewrite = 0) WHERE explain LIKE '%Type: cross%';
SELECT count() FROM ck_l l LEFT ANY JOIN ck_r r ON l.x < r.y
    SETTINGS allow_inequality_join_as_cross_join = 1, allow_block_nested_loop_join = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE ck_l;
DROP TABLE ck_r;
