DROP TABLE IF EXISTS t_materialize_delete;

CREATE TABLE t_materialize_delete (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id PARTITION BY id % 10;

SET mutations_sync = 2;

INSERT INTO t_materialize_delete SELECT number, number FROM numbers(100);

SELECT 'Inserted';

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

SELECT 'Lighweight deleted';

DELETE FROM t_materialize_delete WHERE id % 7 = 3;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

SELECT 'Mask applied';

ALTER TABLE t_materialize_delete APPLY DELETED MASK;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

SELECT 'Lighweight deleted';

DELETE FROM t_materialize_delete WHERE id % 7 = 4;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

SELECT 'Mask applied in partition';

ALTER TABLE t_materialize_delete APPLY DELETED MASK IN PARTITION 5;

SELECT count(), sum(v) FROM t_materialize_delete;
SELECT count(), sum(rows), sum(has_lightweight_delete) FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_delete' AND active;

DROP TABLE t_materialize_delete;
