-- Tags: no-replicated-database
-- no-replicated-database: SYSTEM STOP MERGES works only on one replica.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_shared SYNC;

CREATE TABLE t_shared (id UInt64, c1 UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = true,
    enable_block_offset_column = true;

SYSTEM STOP MERGES t_shared;

INSERT INTO t_shared SELECT number, number FROM numbers(10);

UPDATE t_shared SET c1 = 111 WHERE id % 2 = 1;

INSERT INTO t_shared SELECT number, number FROM numbers(50, 10);

UPDATE t_shared SET c1 = 222 WHERE id % 2 = 0;

SELECT name, rows  FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' ORDER BY name;

SELECT * FROM t_shared ORDER BY id;

DROP TABLE t_shared SYNC;
