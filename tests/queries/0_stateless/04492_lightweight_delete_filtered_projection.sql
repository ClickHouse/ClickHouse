-- Test lightweight DELETE (rebuild mode) preserves a filtered projection's WHERE
DROP TABLE IF EXISTS t_lwd_proj;

CREATE TABLE t_lwd_proj (time DateTime, event_type String, message String)
ENGINE = MergeTree ORDER BY time
SETTINGS lightweight_mutation_projection_mode = 'rebuild';

ALTER TABLE t_lwd_proj ADD PROJECTION proj_pv
(
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

INSERT INTO t_lwd_proj VALUES
    ('2024-01-01 00:00:00', 'pageview', 'a'),
    ('2024-01-02 00:00:00', 'click',    'b'),
    ('2024-01-03 00:00:00', 'pageview', 'c'),
    ('2024-01-04 00:00:00', 'pageview', 'd');

ALTER TABLE t_lwd_proj MATERIALIZE PROJECTION proj_pv SETTINGS mutations_sync = 2;

SELECT 'before', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_proj' AND name = 'proj_pv' AND active;

DELETE FROM t_lwd_proj WHERE message = 'a' SETTINGS lightweight_deletes_sync = 2;

-- After a lightweight delete the projection is rebuilt; the projection WHERE must be
-- AND-combined with _row_exists, so only surviving pageview rows remain (c, d = 2 rows).
SELECT 'after', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_proj' AND name = 'proj_pv' AND active;

SELECT time, message FROM t_lwd_proj WHERE event_type = 'pageview' ORDER BY time
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

DROP TABLE t_lwd_proj;
