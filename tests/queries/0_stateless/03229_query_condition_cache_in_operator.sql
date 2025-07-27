-- Tags: no-parallel
-- no-parallel: Messes with internal cache

SET use_query_condition_cache = 1;

-- Start from a clean query condition cache
SYSTEM DROP QUERY CONDITION CACHE;

SELECT '-- Prepare data';

DROP TABLE IF EXISTS tab, filter_tab;

CREATE TABLE tab
(
    id UInt32 DEFAULT 0,
) ENGINE = MergeTree()
ORDER BY tuple();

SELECT '-- Test IN (subquery)';
CREATE TABLE filter_tab
(
    id UInt32
)
ENGINE = Memory();

INSERT INTO tab
SELECT
    number AS id
FROM numbers(1000000);

INSERT INTO filter_tab VALUES(1);

SELECT '-- First run';
-- At this point, the result returns 1
SELECT count()
FROM tab
WHERE id IN (
    SELECT id
    FROM filter_tab
);

-- Expect empty query condition cache
SELECT count(*) FROM system.query_condition_cache;

-- `filter_tab` adds 1 line of data
INSERT INTO filter_tab VALUES(100001);

SELECT '-- Second run';
-- At this point, the result should be returns 2
SELECT count()
FROM tab
WHERE id IN (
    SELECT id
    FROM filter_tab
);

SELECT '-- Test that the IN operator is in principle cache-able';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT * FROM tab WHERE id IN (1, 100001);
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE IF EXISTS tab, filter_tab;
