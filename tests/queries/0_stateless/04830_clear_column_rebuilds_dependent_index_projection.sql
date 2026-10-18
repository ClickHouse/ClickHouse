-- CLEAR COLUMN recalculates MATERIALIZED columns that depend on the cleared column.
-- Skip indices and projections built over such recalculated columns must be rebuilt
-- as well, otherwise the mutation hardlinks derived data built over the old values.

DROP TABLE IF EXISTS t_clear_rebuild;

CREATE TABLE t_clear_rebuild
(
    a UInt64,
    m UInt64 MATERIALIZED a + 1,
    pad UInt64 DEFAULT 42,
    INDEX idx_m m TYPE minmax GRANULARITY 1,
    PROJECTION p_m (SELECT m, count() GROUP BY m)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_clear_rebuild (a) SELECT number + 10 FROM numbers(10);

SELECT 'before clear';
SELECT count() FROM t_clear_rebuild WHERE m = 11 SETTINGS force_data_skipping_indices = 'idx_m', optimize_use_projections = 0;

ALTER TABLE t_clear_rebuild CLEAR COLUMN a SETTINGS mutations_sync = 1;

SELECT 'after clear';
SELECT DISTINCT a, m FROM t_clear_rebuild;

-- The skip index must have been rebuilt over the recalculated values of `m`:
-- a stale index (built over `m` in [11, 20]) would prune every granule here
-- and return 0 instead of 10.
SELECT count() FROM t_clear_rebuild WHERE m = 1 SETTINGS force_data_skipping_indices = 'idx_m', optimize_use_projections = 0;
SELECT count() FROM t_clear_rebuild WHERE m = 11 SETTINGS force_data_skipping_indices = 'idx_m', optimize_use_projections = 0;

-- The projection must have been rebuilt as well: a stale projection would
-- return the old values of `m`.
SELECT m, count() FROM t_clear_rebuild GROUP BY m ORDER BY m
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_clear_rebuild;
