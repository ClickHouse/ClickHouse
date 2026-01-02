-- Tags: no-fasttest skip_msan skip_s3 skip_parallel skip_tsan no-parallel-replicas


DROP TABLE IF EXISTS test_indexed;

CREATE TABLE test_indexed
(
    id UInt32,
    value String
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_indexed
VALUES (1,'a'),(5,'b'),(10,'c'),(15,'d'),(20,'e');

EXPLAIN indices = 1
SELECT *
FROM test_indexed
WHERE id = 5
FORMAT TSVRaw;

DROP TABLE test_indexed;

