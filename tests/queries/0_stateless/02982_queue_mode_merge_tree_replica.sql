-- Tags: zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

set asterisk_include_materialized_columns=1;

DROP TABLE IF EXISTS repl1;
DROP TABLE IF EXISTS repl2;

CREATE TABLE repl1(a UInt64, b UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/02982', 'repl1') ORDER BY a SETTINGS queue=1;
CREATE TABLE repl2(a UInt64, b UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/02982', 'repl2') ORDER BY a SETTINGS queue=1;

SELECT 'start';
SELECT * FROM repl1;
SELECT * FROM repl2;

SELECT 'insert some data';
INSERT INTO repl1 (*) SELECT number, number FROM numbers(3);
INSERT INTO repl1 (*) SELECT number, number FROM numbers(4);
INSERT INTO repl2 (*) SELECT number, number FROM numbers(5);
INSERT INTO repl2 (*) SELECT number, number FROM numbers(6);

SELECT 'optimize table to create single part';
OPTIMIZE TABLE repl1 FINAL;

SELECT * FROM repl1;

SELECT 'cursor lookup for repl1 partition';
SELECT * FROM repl1 WHERE _queue_replica = 'repl1' AND (_queue_block_number, _queue_block_offset) > (0, 1);

SELECT 'cursor lookup for repl2 partition';
SELECT * FROM repl1 WHERE _queue_replica = 'repl2' AND (_queue_block_number, _queue_block_offset) > (0, 1);

TRUNCATE TABLE repl1;

SELECT 'end';

