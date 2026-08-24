-- The `WHERE` clause of a projection SELECT (`ASTProjectionSelectQuery`) was not serialized
-- to JSON, so a filtered projection silently round-tripped as an unfiltered one.

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8, PROJECTION p (SELECT x WHERE x > 0)) ENGINE = MergeTree ORDER BY x'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8, `y` UInt8, PROJECTION p (SELECT x, sum(y) WHERE y != 3 GROUP BY x ORDER BY x)) ENGINE = MergeTree ORDER BY x'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD PROJECTION p (SELECT x WHERE x IN (1, 2, 3))'));
