CREATE TABLE test_table
(
    dt DateTime,
    id UInt32,
    url String,
    visits UInt32
)
ENGINE ReplacingMergeTree
ORDER BY (dt, id)
PARTITION BY toYYYYMM(dt);

SYSTEM STOP merges test_table;

INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 100);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 101);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 102);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 103);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 104);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 105);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 106);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 107);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 108);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 109);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 110);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 111);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 112);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 113);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 114);
INSERT INTO test_table VALUES (toDate('2024-10-24'), 1, '/index', 115);

ALTER TABLE test_table DETACH PARTITION 202410;
ALTER TABLE test_table ATTACH PARTITION 202410;

SELECT id, visits FROM test_table FINAL ORDER BY id FORMAT Vertical;