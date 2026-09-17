-- MODIFY PROJECTION accepts both declaration shapes of ParserProjectionDeclaration:
-- a `(SELECT ...)` projection and a projection index `INDEX <expr> TYPE <type>`.
-- this test covers the projection-index shape, whose metadata is built through ProjectionIndexFactory
-- instead of fillProjectionDescriptionByQuery.

DROP TABLE IF EXISTS t_modify_index_projection;

CREATE TABLE t_modify_index_projection
(
    region String,
    v UInt64,
    PROJECTION p INDEX region TYPE basic WITH SETTINGS (index_granularity = 1024)
)
ENGINE = MergeTree ORDER BY v
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

-- The test asserts that the two inserted parts stay separate until OPTIMIZE, so a
-- spontaneous background merge must not combine them earlier.
SYSTEM STOP MERGES t_modify_index_projection;

INSERT INTO t_modify_index_projection SELECT toString(number % 10), number FROM numbers(10000);

SELECT '-- initial definition';
SHOW CREATE TABLE t_modify_index_projection;

SELECT '-- modify projection settings';
ALTER TABLE t_modify_index_projection MODIFY PROJECTION p INDEX region TYPE basic WITH SETTINGS (index_granularity = 128);

SELECT '-- new definition reflects the new setting';
SHOW CREATE TABLE t_modify_index_projection;

SELECT '-- the old part keeps the old granularity, a new part gets the new one';
INSERT INTO t_modify_index_projection SELECT toString(number % 10), number FROM numbers(10000);

-- `name` is the projection name and is the same for both parts, so order by the
-- parent part name to make the output deterministic.
SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_index_projection' AND active
ORDER BY parent_name;

SELECT '-- a merge rebuilds the projection with the new granularity';
SYSTEM START MERGES t_modify_index_projection;
OPTIMIZE TABLE t_modify_index_projection FINAL;

SELECT name, rows, marks
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_modify_index_projection' AND active
ORDER BY parent_name;

SELECT '-- errors';
-- Only the WITH SETTINGS clause may change: the index expression and the declaration
-- shape (switching between INDEX and SELECT) are part of the compared definition.
ALTER TABLE t_modify_index_projection MODIFY PROJECTION p INDEX v TYPE basic WITH SETTINGS (index_granularity = 128); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_index_projection MODIFY PROJECTION p (SELECT region ORDER BY region) WITH SETTINGS (index_granularity = 128); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_modify_index_projection MODIFY PROJECTION p INDEX region TYPE basic WITH SETTINGS (old_parts_lifetime = 100); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_modify_index_projection;
