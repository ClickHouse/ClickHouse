-- `ALTER TABLE ... MATERIALIZE TTL` reads only what it needs, so recomputing a `MATERIALIZED` column
-- there also needs the columns its expression reads next to the expired one, and the columns read by a
-- projection over the recomputed column.

SET materialize_ttl_after_modify = 0;

-- `y` is neither a TTL target nor an input of a TTL expression, but the recompute of `m` needs it.
DROP TABLE IF EXISTS t_materialize_ttl_extra_inputs;
CREATE TABLE t_materialize_ttl_extra_inputs
(
    d DateTime,
    x Int32,
    y Int32,
    m Int32 MATERIALIZED x + y
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_extra_inputs (d, x, y) VALUES ('2000-01-01 00:00:00', 41, 1000);
SELECT x, y, m FROM t_materialize_ttl_extra_inputs;

ALTER TABLE t_materialize_ttl_extra_inputs
    MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_extra_inputs MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT x, y, m FROM t_materialize_ttl_extra_inputs;

DROP TABLE t_materialize_ttl_extra_inputs;

-- A projection grouping by a column the TTL does not touch, over a recomputed one. Rebuilt without `z`
-- in the block, the missing column is filled with a type default and the projection answers with a
-- group key the base table does not have.
DROP TABLE IF EXISTS t_materialize_ttl_extra_projection;
CREATE TABLE t_materialize_ttl_extra_projection
(
    d DateTime,
    x Int32,
    z Int32,
    m Int32 MATERIALIZED x + 1,
    PROJECTION p (SELECT z, sum(m) GROUP BY z)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_materialize_ttl_extra_projection (d, x, z) VALUES ('2000-01-01 00:00:00', 41, 7);

ALTER TABLE t_materialize_ttl_extra_projection
    MODIFY COLUMN x Int32 TTL d + INTERVAL 1 SECOND SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize_ttl_extra_projection MATERIALIZE TTL SETTINGS mutations_sync = 2;

SELECT z, sum(m) FROM t_materialize_ttl_extra_projection GROUP BY z
SETTINGS optimize_use_projections = 0;
SELECT z, sum(m) FROM t_materialize_ttl_extra_projection GROUP BY z
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_materialize_ttl_extra_projection;
