--https://github.com/ClickHouse/ClickHouse/issues/5323
CREATE TABLE test_bug_optimization
(
    `path` String
)
ENGINE = MergeTree
ORDER BY path;

WITH (path = 'test1') OR match(path, 'test2') OR (match(path, 'test3') AND match(path, 'test2')) OR match(path, 'test4') OR (path = 'test5') OR (path = 'test6') AS alias_in_error
SELECT count(1)
FROM test_bug_optimization
WHERE alias_in_error;
