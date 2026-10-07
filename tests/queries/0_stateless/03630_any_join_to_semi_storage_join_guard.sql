-- Regression test for issue #103318:
-- ANY LEFT JOIN on a Join engine table with a WHERE filter on a right-side payload column
-- threw INCOMPATIBLE_TYPE_OF_JOIN, because the ANY -> SEMI/ANTI conversion mutated the
-- StorageJoin declared strictness, which StorageJoin::getJoinLocked rejects.

SET enable_analyzer = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1; -- CI may inject False; pin so the conversion pass (whose StorageJoin guard is under test) always runs
SET join_use_nulls = 0; -- CI may inject True; the Join engine rejects a mismatched join_use_nulls, which is a different error unrelated to this test

DROP TABLE IF EXISTS storage_join_103318;
CREATE TABLE storage_join_103318 (id UInt64, val String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO storage_join_103318 VALUES (1, 'x');

-- The query used to throw INCOMPATIBLE_TYPE_OF_JOIN. It must now return a result.
SELECT count()
FROM (SELECT 1 :: UInt64 AS id) AS t
ANY LEFT JOIN storage_join_103318 AS j USING (id)
WHERE j.val != '';

-- The conversion must be declined for a StorageJoin: the join stays ANY (no SEMI).
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT 1 :: UInt64 AS id) AS t
    ANY LEFT JOIN storage_join_103318 AS j USING (id)
    WHERE j.val != ''
)
WHERE explain ILIKE '%Strictness: semi%';

-- The conversion must still fire for a non-StorageJoin right child (guard must not over-fire):
-- the same shape over a MergeTree table is rewritten to a SEMI join.
DROP TABLE IF EXISTS mt_103318;
CREATE TABLE mt_103318 (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt_103318 VALUES (1, 'x');
SELECT count()
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT 1 :: UInt64 AS id) AS t
    ANY LEFT JOIN mt_103318 AS j USING (id)
    WHERE j.val != ''
)
WHERE explain ILIKE '%Strictness: semi%';

DROP TABLE storage_join_103318;
DROP TABLE mt_103318;
