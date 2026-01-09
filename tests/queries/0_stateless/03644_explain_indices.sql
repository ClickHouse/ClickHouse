-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS test_indexed;

CREATE TABLE test_indexed
(
    id UInt32,
    value String
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_indexed VALUES (1,'a'),(5,'b'),(10,'c'),(15,'d'),(20,'e');

EXPLAIN indices = 1
SELECT *
FROM test_indexed
WHERE id = 5;

DROP TABLE test_indexed;
