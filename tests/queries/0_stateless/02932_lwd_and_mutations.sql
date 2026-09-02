DROP TABLE IF EXISTS t_lwd_mutations;

CREATE TABLE t_lwd_mutations(id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lwd_mutations SELECT number, 0 FROM numbers(1000);

SET mutations_sync = 2;

DELETE FROM t_lwd_mutations WHERE id % 10 = 0;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

ALTER TABLE t_lwd_mutations UPDATE v = 1 WHERE id % 4 = 0, DELETE WHERE id % 10 = 1;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

DELETE FROM t_lwd_mutations WHERE id % 10 = 2;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

ALTER TABLE t_lwd_mutations UPDATE v = 1 WHERE id % 4 = 1, DELETE WHERE id % 10 = 3;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

ALTER TABLE t_lwd_mutations UPDATE _row_exists = 0 WHERE id % 10 = 4, DELETE WHERE id % 10 = 5;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

ALTER TABLE t_lwd_mutations DELETE WHERE id % 10 = 6, UPDATE _row_exists = 0 WHERE id % 10 = 7;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

ALTER TABLE t_lwd_mutations APPLY DELETED MASK;

SELECT count(), sum(v), arraySort(groupUniqArray(id % 10)) FROM t_lwd_mutations;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_mutations' AND active;

DROP TABLE IF EXISTS t_lwd_mutations;
