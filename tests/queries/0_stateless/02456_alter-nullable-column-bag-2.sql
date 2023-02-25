DROP TABLE IF EXISTS t1 SYNC;
CREATE TABLE t1  (v UInt64) ENGINE=ReplicatedMergeTree('/test/tables/{database}/test/t1', 'r1') ORDER BY v PARTITION BY v;
INSERT INTO t1 values(1);
ALTER TABLE t1 ADD COLUMN s String;
INSERT INTO t1 values(1, '1');
ALTER TABLE t1 MODIFY COLUMN s Nullable(String);
-- SELECT _part, * FROM t1;

alter table t1 detach partition 1;

SELECT _part, * FROM t1;
--0 rows in set. Elapsed: 0.001 sec.

alter table t1 attach partition 1;
select count() from t1;

