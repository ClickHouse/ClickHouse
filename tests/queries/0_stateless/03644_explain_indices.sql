-- Tags: no-fasttest

CREATE TABLE IF NOT EXISTS test_indexed
(
    id UInt32,
    value String
) ENGINE = MergeTree()
    ORDER BY id;

INSERT INTO test_indexed VALUES
                             (1,'a'),(5,'b'),(10,'c'),(15,'d'),(20,'e');

EXPLAIN indices = 1
SELECT *
FROM test_indexed
WHERE id = 5