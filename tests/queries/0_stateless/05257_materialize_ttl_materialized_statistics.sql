-- `ALTER TABLE ... MATERIALIZE TTL` on a wide part rewrites a column TTL target and the `MATERIALIZED`
-- columns computed from it, so the statistics of those columns have to be rebuilt too - carried over
-- from the source part, they would describe the values the TTL has just reset.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_materialize_ttl_statistics;
CREATE TABLE t_materialize_ttl_statistics
(
    d DateTime,
    x Int32 STATISTICS(basic),
    m Int32 MATERIALIZED x + 1 STATISTICS(basic),
    -- Untouched by the TTL, so the command rewrites only some of the columns and hardlinks the rest.
    s String
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_statistics (d, x, s) VALUES ('2000-01-01 00:00:00', 41, 'a');
SELECT column, estimates.min, estimates.max FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_materialize_ttl_statistics' AND active AND column IN ('x', 'm') ORDER BY column;

-- Add the TTL without applying it, so that the command below is the only thing that can.
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_materialize_ttl_statistics MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_statistics MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT x, m, s FROM t_materialize_ttl_statistics;
SELECT column, estimates.min, estimates.max FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_materialize_ttl_statistics' AND active AND column IN ('x', 'm') ORDER BY column;

DROP TABLE t_materialize_ttl_statistics;
