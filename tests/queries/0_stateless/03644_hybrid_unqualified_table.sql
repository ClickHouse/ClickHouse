SET allow_experimental_hybrid_table = 1;

SELECT 'Hybrid allows unqualified local tables by default';

DROP TABLE IF EXISTS test_hybrid_unqualified_segment SYNC;
DROP TABLE IF EXISTS test_hybrid_unqualified SYNC;

CREATE TABLE test_hybrid_unqualified_segment
(
    `number` UInt64
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO test_hybrid_unqualified_segment VALUES (10), (20);

CREATE TABLE test_hybrid_unqualified
(
    `number` UInt64
)
ENGINE = Hybrid(
    remote('localhost:9000', system.numbers), number = 0,
    test_hybrid_unqualified_segment, number >= 10
);

SELECT count() FROM test_hybrid_unqualified;

SELECT positionCaseInsensitive(engine_full, concat(currentDatabase(), '.test_hybrid_unqualified_segment')) > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 'test_hybrid_unqualified';

DROP TABLE IF EXISTS test_hybrid_unqualified SYNC;
DROP TABLE IF EXISTS test_hybrid_unqualified_segment SYNC;
