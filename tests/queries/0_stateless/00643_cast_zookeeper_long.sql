-- Tags: long, zookeeper

SET database_atomic_wait_for_drop_and_detach_synchronously=1;

DROP TABLE IF EXISTS cast1;
DROP TABLE IF EXISTS cast2;

CREATE TABLE cast1
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
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00643/cast', 'r1') ORDER BY e;

SHOW CREATE TABLE cast1 FORMAT TSVRaw;
DESC TABLE cast1;

INSERT INTO cast1 (x) VALUES (1);
SELECT * FROM cast1;

CREATE TABLE cast2 AS cast1 ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_00643/cast', 'r2') ORDER BY e;

SYSTEM SYNC REPLICA cast2;

SELECT * FROM cast2;

DROP TABLE cast1;
DROP TABLE cast2;
