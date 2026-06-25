DROP TABLE IF EXISTS t_packed_idx_collision;

CREATE TABLE t_packed_idx_collision (a UInt64, b UInt64, c String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_full_part_storage = 100000000, min_bytes_for_wide_part = 100000000;

INSERT INTO t_packed_idx_collision SELECT number, number, toString(number) FROM numbers(200);

SELECT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed_idx_collision' AND active;
ALTER TABLE t_packed_idx_collision ADD INDEX idx1 b TYPE minmax GRANULARITY 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_idx_collision ADD INDEX idx10 c TYPE set(0) GRANULARITY 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_idx_collision MATERIALIZE INDEX idx10 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed_idx_collision UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT count(), sum(b) FROM t_packed_idx_collision;

DROP TABLE t_packed_idx_collision;
