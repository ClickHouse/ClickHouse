DROP TABLE IF EXISTS replicated_table_for_alter1;
DROP TABLE IF EXISTS replicated_table_for_alter2;

SET replication_alter_partitions_sync = 2;

CREATE TABLE replicated_table_for_alter1 (
  id UInt64,
  Data String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_table_for_alter', '1') ORDER BY id;

CREATE TABLE replicated_table_for_alter2 (
  id UInt64,
  Data String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_table_for_alter', '2') ORDER BY id;

SHOW CREATE TABLE replicated_table_for_alter1;

ALTER TABLE replicated_table_for_alter1 MODIFY SETTING index_granularity = 4096; -- { serverError 472 }

SHOW CREATE TABLE replicated_table_for_alter1;

INSERT INTO replicated_table_for_alter2 VALUES (1, '1'), (2, '2');

SYSTEM SYNC REPLICA replicated_table_for_alter1;

ALTER TABLE replicated_table_for_alter1 MODIFY SETTING use_minimalistic_part_header_in_zookeeper = 1;

INSERT INTO replicated_table_for_alter1 VALUES (3, '3'), (4, '4');

SYSTEM SYNC REPLICA replicated_table_for_alter2;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter2;
ATTACH TABLE replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter1;
ATTACH TABLE replicated_table_for_alter1;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

ALTER TABLE replicated_table_for_alter2 MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;
INSERT INTO replicated_table_for_alter2 VALUES (3, '1'), (4, '2'); -- { serverError 252 }

INSERT INTO replicated_table_for_alter1 VALUES (5, '5'), (6, '6');

SYSTEM SYNC REPLICA replicated_table_for_alter2;

SELECT COUNT() FROM replicated_table_for_alter1;
SELECT COUNT() FROM replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter2;
ATTACH TABLE replicated_table_for_alter2;

DETACH TABLE replicated_table_for_alter1;
ATTACH TABLE replicated_table_for_alter1;

SHOW CREATE TABLE replicated_table_for_alter1;
SHOW CREATE TABLE replicated_table_for_alter2;

ALTER TABLE replicated_table_for_alter1 ADD COLUMN Data2 UInt64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15;

SHOW CREATE TABLE replicated_table_for_alter1;
SHOW CREATE TABLE replicated_table_for_alter2;

DROP TABLE IF EXISTS replicated_table_for_alter2;
DROP TABLE IF EXISTS replicated_table_for_alter1;
