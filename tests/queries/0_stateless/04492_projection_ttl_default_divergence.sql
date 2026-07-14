-- A column with a non-type DDL DEFAULT and a column TTL, read through a projection, must return
-- the DDL default (not the column type's default) once the TTL has expired. Regression test for the
-- wide-part fully-expired fast path in TTLColumnAlgorithm, which reset the column to the type default
-- (e.g. 0); a rebuilt projection materialized that placeholder while the base read the DDL default
-- (e.g. -1) via expired_columns, so a projection-served read diverged from the base-table read.

DROP TABLE IF EXISTS t_proj_ttl_wide;

CREATE TABLE t_proj_ttl_wide
(
    d Date,
    k UInt32,
    v Int32 DEFAULT -1 TTL d + INTERVAL 1 DAY,       -- constant DDL default
    w Int32 DEFAULT k + 1000 TTL d + INTERVAL 1 DAY, -- per-row DDL default expression
    PROJECTION p (SELECT k, v, w ORDER BY k)
)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0; -- force WIDE part

INSERT INTO t_proj_ttl_wide SELECT '2000-01-01', number, 100 + number, 500 + number FROM numbers(1000);
OPTIMIZE TABLE t_proj_ttl_wide FINAL; -- applies the column TTL and rebuilds the projection

-- Constant DDL default: base and projection must both read -1.
SELECT 'wide v base', min(v), max(v) FROM t_proj_ttl_wide SETTINGS optimize_use_projections = 0;
SELECT 'wide v proj', min(v), max(v) FROM t_proj_ttl_wide SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- Per-row DDL default expression: base and projection must both read k + 1000 for every row.
SELECT 'wide w base', sum(w = k + 1000), count() FROM t_proj_ttl_wide SETTINGS optimize_use_projections = 0;
SELECT 'wide w proj', sum(w = k + 1000), count() FROM t_proj_ttl_wide SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_ttl_wide;

-- Control: a compact part already used the DDL default (slow path); it must stay consistent.
DROP TABLE IF EXISTS t_proj_ttl_compact;

CREATE TABLE t_proj_ttl_compact
(
    d Date,
    k UInt32,
    v Int32 DEFAULT -1 TTL d + INTERVAL 1 DAY,
    PROJECTION p (SELECT k, v ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000; -- force COMPACT part

INSERT INTO t_proj_ttl_compact SELECT '2000-01-01', number, 100 + number FROM numbers(1000);
OPTIMIZE TABLE t_proj_ttl_compact FINAL;

SELECT 'compact v base', min(v), max(v) FROM t_proj_ttl_compact SETTINGS optimize_use_projections = 0;
SELECT 'compact v proj', min(v), max(v) FROM t_proj_ttl_compact SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_ttl_compact;

-- Must-not-regress: a column with no DDL DEFAULT keeps the column type's default (0) after expiry.
DROP TABLE IF EXISTS t_proj_ttl_nodefault;

CREATE TABLE t_proj_ttl_nodefault
(
    d Date,
    k UInt32,
    u Int32 TTL d + INTERVAL 1 DAY, -- no DDL DEFAULT -> type default 0
    PROJECTION p (SELECT k, u ORDER BY k)
)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0; -- force WIDE part

INSERT INTO t_proj_ttl_nodefault SELECT '2000-01-01', number, 100 + number FROM numbers(1000);
OPTIMIZE TABLE t_proj_ttl_nodefault FINAL;

SELECT 'nodefault u base', min(u), max(u) FROM t_proj_ttl_nodefault SETTINGS optimize_use_projections = 0;
SELECT 'nodefault u proj', min(u), max(u) FROM t_proj_ttl_nodefault SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_ttl_nodefault;
