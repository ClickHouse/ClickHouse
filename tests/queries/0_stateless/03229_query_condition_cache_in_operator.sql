-- Tags: no-parallel, long, no-asan, no-ubsan, no-debug
-- no-parallel: Messes with internal cache
-- Other tags because the test generates 100k rows which is slow

-- Test for issue #84508 (recursive CTEs return wrong results if the query condition cache is on)

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

-- Expect empty query condition cache
SELECT count(*) FROM system.query_condition_cache;

SELECT '-- Modify row in "in_tab" from "uuid10" to "uuid10000"';
ALTER TABLE in_tab UPDATE uuid = 'uuid10000' WHERE uuid = 'uuid10' SETTINGS mutations_sync = 2;

SELECT '-- Second run';

-- Same query as before
SELECT * FROM tab WHERE uuid IN (SELECT uuid FROM in_tab);

DROP TABLE in_tab;

SELECT '-- Test that the IN operator is in principle cache-able';

SYSTEM DROP QUERY CONDITION CACHE;

SELECT * FROM tab WHERE uuid IN ('uuid10');
SELECT count(*) FROM system.query_condition_cache;

DROP TABLE tab;
