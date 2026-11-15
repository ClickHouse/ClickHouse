-- `ALTER TABLE ... MATERIALIZE TTL` is the command that applies a column TTL to a part no merge is
-- going to touch, so it has to rewrite the `MATERIALIZED` columns computed from the expired column too -
-- which means reading and writing them, not only the TTL targets.

DROP TABLE IF EXISTS t_materialize_ttl_materialized;
CREATE TABLE t_materialize_ttl_materialized
(
    d DateTime,
    x Int32,
    m1 Int32 MATERIALIZED x + 1,
    m2 Int32 MATERIALIZED m1 + 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_materialized (d, x) VALUES ('2000-01-01 00:00:00', 41);
SELECT x, m1, m2 FROM t_materialize_ttl_materialized;

-- Add the TTL without applying it, so that the command below is the only thing that can.
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_materialize_ttl_materialized MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_materialized MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- A single part with the TTL already applied: nothing else is going to repair these columns.
SELECT x, m1, m2 FROM t_materialize_ttl_materialized;

DROP TABLE t_materialize_ttl_materialized;
