DROP TABLE IF EXISTS test.cast1;
DROP TABLE IF EXISTS test.cast2;

CREATE TABLE test.cast1
(
    x UInt8,
    e Enum8
    (
        'hello' = 1,
        'world' = 2
    )
    DEFAULT
    CAST
    (
        x
        AS
        Enum8
        (
            'hello' = 1,
            'world' = 2
        )
    )
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_cast', 'r1') ORDER BY e;

SHOW CREATE TABLE test.cast1 FORMAT TSVRaw;
DESC TABLE test.cast1;

INSERT INTO test.cast1 (x) VALUES (1);
SELECT * FROM test.cast1;

CREATE TABLE test.cast2 AS test.cast1 ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_cast', 'r2') ORDER BY e;

SYSTEM SYNC REPLICA test.cast2;

SELECT * FROM test.cast2;

DROP TABLE test.cast1;
DROP TABLE test.cast2;
