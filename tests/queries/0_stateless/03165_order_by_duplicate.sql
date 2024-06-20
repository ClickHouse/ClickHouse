CREATE TABLE test
ENGINE = ReplacingMergeTree
PRIMARY KEY id
AS SELECT number AS id FROM numbers(100);

EXPLAIN QUERY TREE SELECT id
FROM test FINAL
WHERE id IN (
    SELECT DISTINCT id
    FROM test FINAL
    ORDER BY id ASC
    LIMIT 4
)
ORDER BY id ASC
LIMIT 1 BY id
SETTINGS allow_experimental_analyzer = 1;
