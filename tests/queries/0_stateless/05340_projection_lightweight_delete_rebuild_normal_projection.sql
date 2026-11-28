-- A normal projection without `WHERE` rebuilt by a lightweight delete must not keep the deleted rows.

SET lightweight_deletes_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_lwd_normal_proj;

CREATE TABLE t_lwd_normal_proj
(
    uid UInt32,
    age UInt32,
    PROJECTION p (SELECT uid, age ORDER BY age)
)
ENGINE = MergeTree ORDER BY uid
SETTINGS lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_lwd_normal_proj SELECT number, number % 10 FROM numbers(100);

DELETE FROM t_lwd_normal_proj WHERE uid % 2 = 0;

SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_normal_proj' AND name = 'p' AND active;

SELECT count(), sum(uid) FROM t_lwd_normal_proj WHERE age >= 0
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

SELECT count(), sum(uid) FROM t_lwd_normal_proj WHERE age = 3
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_lwd_normal_proj;
