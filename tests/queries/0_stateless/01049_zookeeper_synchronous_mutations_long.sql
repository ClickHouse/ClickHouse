-- Tags: long, zookeeper

DROP TABLE IF EXISTS table_for_synchronous_mutations1;
DROP TABLE IF EXISTS table_for_synchronous_mutations2;

SELECT 'Replicated';

CREATE TABLE table_for_synchronous_mutations1(k UInt32, v1 UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01049/table_for_synchronous_mutations', '1') ORDER BY k;

CREATE TABLE table_for_synchronous_mutations2(k UInt32, v1 UInt64) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01049/table_for_synchronous_mutations', '2') ORDER BY k;

INSERT INTO table_for_synchronous_mutations1 select number, number from numbers(100000);

SYSTEM SYNC REPLICA table_for_synchronous_mutations2;

ALTER TABLE table_for_synchronous_mutations1 UPDATE v1 = v1 + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT is_done FROM system.mutations where database = currentDatabase() and table = 'table_for_synchronous_mutations1';

-- Another mutation, just to be sure, that previous finished
ALTER TABLE table_for_synchronous_mutations1 UPDATE v1 = v1 + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT is_done FROM system.mutations where database = currentDatabase() and table = 'table_for_synchronous_mutations1';

DROP TABLE IF EXISTS table_for_synchronous_mutations1;
DROP TABLE IF EXISTS table_for_synchronous_mutations2;

SELECT 'Normal';

DROP TABLE IF EXISTS table_for_synchronous_mutations_no_replication;

CREATE TABLE table_for_synchronous_mutations_no_replication(k UInt32, v1 UInt64) ENGINE MergeTree ORDER BY k;

INSERT INTO table_for_synchronous_mutations_no_replication select number, number from numbers(100000);

ALTER TABLE table_for_synchronous_mutations_no_replication UPDATE v1 = v1 + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT is_done FROM system.mutations where database = currentDatabase() and table = 'table_for_synchronous_mutations_no_replication';

-- Another mutation, just to be sure, that previous finished
ALTER TABLE table_for_synchronous_mutations_no_replication UPDATE v1 = v1 + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT is_done FROM system.mutations where database = currentDatabase() and table = 'table_for_synchronous_mutations_no_replication';

DROP TABLE IF EXISTS table_for_synchronous_mutations_no_replication;
