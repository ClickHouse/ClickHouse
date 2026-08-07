-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge

DROP TABLE IF EXISTS t_lightweight_mut_6;

SET apply_mutations_on_fly = 1;

CREATE TABLE t_lightweight_mut_6 (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lightweight_mut_6', '1')
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0; -- There is a bug.

INSERT INTO t_lightweight_mut_6 select number, number FROM numbers(10000);

SET mutations_sync = 2;
DELETE FROM t_lightweight_mut_6 WHERE id % 2 = 0;

SELECT count(), sum(v) FROM t_lightweight_mut_6;

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lightweight_mut_6' AND active;

SET mutations_sync = 0;
SYSTEM STOP MERGES t_lightweight_mut_6;

ALTER TABLE t_lightweight_mut_6 UPDATE v = v * 2 WHERE id % 5 = 0;
ALTER TABLE t_lightweight_mut_6 DELETE WHERE id % 3 = 0;

SYSTEM SYNC REPLICA t_lightweight_mut_6 PULL;

SELECT count(), sum(v) FROM t_lightweight_mut_6;
SELECT count(), sum(v) FROM t_lightweight_mut_6 SETTINGS apply_mutations_on_fly = 0;

SELECT count(), sum(v) FROM t_lightweight_mut_6 PREWHERE id % 5 = 0;
SELECT count(), sum(v) FROM t_lightweight_mut_6 PREWHERE id % 5 = 0 SETTINGS apply_mutations_on_fly = 0;

SYSTEM START MERGES t_lightweight_mut_6;

SET mutations_sync = 2;
ALTER TABLE t_lightweight_mut_6 UPDATE v = v WHERE 1;
OPTIMIZE TABLE t_lightweight_mut_6 FINAL;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_lightweight_mut_6' AND NOT is_done;

SELECT sum(has_lightweight_delete) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lightweight_mut_6' AND active;

SELECT count(), sum(v) FROM t_lightweight_mut_6;
SELECT count(), sum(v) FROM t_lightweight_mut_6 PREWHERE id % 5 = 0;

DROP TABLE t_lightweight_mut_6;
