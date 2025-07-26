-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_analyzer = 1; 
SET use_query_condition_cache = 1; 

-- Start from a clean query condition cache
SYSTEM DROP QUERY CONDITION CACHE;

SELECT '-- Prepare data';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id String,
    uuid String,
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab (id, uuid) SELECT toString(number), concat('uuid', number) FROM numbers(100000);

SELECT '-- Test IN (subquery)';
CREATE TABLE in_tab
(
    uuid String
)
ENGINE = Memory();

INSERT INTO in_tab VALUES ('uuid10');

SELECT '-- First run';

SELECT * FROM tab WHERE uuid IN (SELECT uuid FROM in_tab);

SELECT '-- Cache is empty';
SELECT count(*) FROM system.query_condition_cache;

SELECT '-- Modify the data in the `in_tab` table to `uuid10000`';
ALTER TABLE in_tab UPDATE uuid = 'uuid10000' WHERE uuid = 'uuid10';

SELECT '-- Second run'; -- same query
SELECT * FROM tab WHERE uuid IN (SELECT uuid FROM in_tab);

SELECT '-- Test IN (values)';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT * FROM tab WHERE uuid IN ('uuid10');

SELECT '-- Cache is not empty';
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
