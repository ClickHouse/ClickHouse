-- Tags: long

DROP TABLE IF EXISTS replicated_table_r1 SYNC;
DROP TABLE IF EXISTS replicated_table_r2 SYNC;

CREATE TABLE replicated_table_r1(id Int32, name String) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_rep','1') ORDER BY id;
CREATE TABLE replicated_table_r2(id Int32, name String) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_rep','2') ORDER BY id;

INSERT INTO replicated_table_r1 select number, toString(number) FROM numbers(100);

SET mutations_sync = 0;

DELETE FROM replicated_table_r1 WHERE id = 10;

SELECT COUNT() FROM replicated_table_r1;
SELECT COUNT() FROM replicated_table_r2;

DELETE FROM replicated_table_r2 WHERE name IN ('1','2','3','4');

SELECT COUNT() FROM replicated_table_r1;

DELETE FROM replicated_table_r1 WHERE 1;

SELECT COUNT() FROM replicated_table_r1;
SELECT COUNT() FROM replicated_table_r2;

DROP TABLE IF EXISTS replicated_table_r1 SYNC;
DROP TABLE IF EXISTS replicated_table_r2 SYNC;

DROP TABLE IF EXISTS t_light_r1 SYNC;
DROP TABLE IF EXISTS t_light_r2 SYNC;

CREATE TABLE t_light_r1(a int, b int, c int, index i_c(b) TYPE minmax granularity 4) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_light','1') ORDER BY a PARTITION BY c % 5;
CREATE TABLE t_light_r2(a int, b int, c int, index i_c(b) TYPE minmax granularity 4) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_light','2') ORDER BY a PARTITION BY c % 5;

INSERT INTO t_light_r1 SELECT number, number, number FROM numbers(10);

DELETE FROM t_light_r1 WHERE c%5=1;
DELETE FROM t_light_r2 WHERE c=4;

SELECT '-----Check that select and merge with lightweight delete.-----';
SELECT count(*) FROM t_light_r1;
SELECT * FROM t_light_r1 ORDER BY a;
SELECT * FROM t_light_r2 ORDER BY a;

OPTIMIZE TABLE t_light_r1 FINAL SETTINGS mutations_sync = 2;
SELECT count(*) FROM t_light_r1;

DROP TABLE IF EXISTS t_light_r1 SYNC;
DROP TABLE IF EXISTS t_light_r2 SYNC;

CREATE TABLE t_light_sync_r1(a int, b int, c int, index i_c(b) TYPE minmax granularity 4) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_sync','1') ORDER BY a PARTITION BY c % 5 SETTINGS min_bytes_for_wide_part=0;

INSERT INTO t_light_sync_r1 SELECT number, number, number FROM numbers(10);

DELETE FROM t_light_sync_r1 WHERE c%3=1;

SELECT '-----Check fetch part with lightweight delete-----';
CREATE TABLE t_light_sync_r2(a int, b int, c int, index i_c(b) TYPE minmax granularity 4) ENGINE = ReplicatedMergeTree('/test/02352/{database}/t_sync','2') ORDER BY a PARTITION BY c % 5 SETTINGS min_bytes_for_wide_part=0;
SYSTEM SYNC REPLICA t_light_sync_r2;

SELECT * FROM t_light_sync_r2 ORDER BY a;

DROP TABLE IF EXISTS t_light_sync_r1 SYNC;
DROP TABLE IF EXISTS t_light_sync_r2 SYNC;
