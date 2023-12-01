DROP TABLE IF EXISTS t_materialize_delete;

CREATE TABLE t_materialize_delete (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() settings min_bytes_for_wide_part = 0;

SET mutations_sync = 2;

INSERT INTO t_materialize_delete SELECT number, number FROM numbers(10);

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT name, rows, has_lightweight_delete FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

DELETE FROM t_materialize_delete WHERE id % 3 = 1;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT name, rows, has_lightweight_delete FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

ALTER TABLE t_materialize_delete APPLY DELETED MASK;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT name, rows, has_lightweight_delete FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

DROP TABLE t_materialize_delete;
