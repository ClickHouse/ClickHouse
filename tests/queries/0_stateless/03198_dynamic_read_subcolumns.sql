-- Tags: no-random-settings, no-object-storage, no-parallel
-- no-parallel: Running `DROP MARK CACHE` can have a big impact on other concurrent tests
-- Tag no-object-storage: this test relies on the number of opened files in MergeTree that can differ in object storages

SET allow_experimental_dynamic_type = 1;
DROP TABLE IF EXISTS test_dynamic;
CREATE TABLE test_dynamic (id UInt64, d Dynamic) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO test_dynamic VALUES (1, 'foo'), (2, 1111), (3, [1, 2, 3]);
EXPLAIN QUERY TREE SELECT d.String FROM test_dynamic SETTINGS allow_experimental_analyzer = 1;
SYSTEM DROP MARK CACHE;
SELECT d.String FROM test_dynamic SETTINGS allow_experimental_analyzer = 1;
SYSTEM DROP MARK CACHE;
SELECT d.String FROM test_dynamic SETTINGS allow_experimental_analyzer = 0;
SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 2) AND (query LIKE 'SELECT d.String %test_dynamic%') AND (current_database = currentDatabase())
ORDER BY event_time_microseconds DESC
LIMIT 2;

DROP TABLE test_dynamic;

