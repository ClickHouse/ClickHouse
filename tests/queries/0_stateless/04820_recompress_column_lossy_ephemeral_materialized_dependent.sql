-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` must see a stored MATERIALIZED
-- column that depends on the recompressed column only through an EPHEMERAL helper: mutations
-- cannot recalculate such a dependent at all (EPHEMERAL values are unavailable during mutations),
-- so the dependency has to be expanded through the EPHEMERAL default expression and rejected.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;

-- A MATERIALIZED column depends on the lossy column through one EPHEMERAL helper: rejected.
DROP TABLE IF EXISTS t_recompress_lossy_ephemeral;
CREATE TABLE t_recompress_lossy_ephemeral
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    eph Float64 EPHEMERAL val * 2,
    mat Float64 MATERIALIZED eph + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_ephemeral (key, val) SELECT number, sin(number / 100.) * 50 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_ephemeral RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_ephemeral;

-- Through a chain of two EPHEMERAL helpers: still rejected.
DROP TABLE IF EXISTS t_recompress_lossy_ephemeral_chain;
CREATE TABLE t_recompress_lossy_ephemeral_chain
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    eph1 Float64 EPHEMERAL val * 2,
    eph2 Float64 EPHEMERAL eph1 + 3,
    mat Float64 MATERIALIZED eph2 + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_ephemeral_chain (key, val) SELECT number, sin(number / 100.) * 50 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_ephemeral_chain RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

-- Removing the MATERIALIZED property in the same ALTER unblocks the recompression.
ALTER TABLE t_recompress_lossy_ephemeral_chain MODIFY COLUMN mat REMOVE MATERIALIZED, RECOMPRESS COLUMN val;
SELECT 'chain after remove materialized', count(), sum(key) FROM t_recompress_lossy_ephemeral_chain;

DROP TABLE t_recompress_lossy_ephemeral_chain;

-- A MATERIALIZED column whose EPHEMERAL input does not read the lossy column is not affected.
DROP TABLE IF EXISTS t_recompress_lossy_ephemeral_unrelated;
CREATE TABLE t_recompress_lossy_ephemeral_unrelated
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    eph UInt64 EPHEMERAL key * 2,
    mat UInt64 MATERIALIZED eph + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_ephemeral_unrelated (key, val) SELECT number, sin(number / 100.) * 50 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_ephemeral_unrelated RECOMPRESS COLUMN val;
SELECT 'unrelated ephemeral', count(), countIf(mat = key * 2 + 1) FROM t_recompress_lossy_ephemeral_unrelated;

DROP TABLE t_recompress_lossy_ephemeral_unrelated;
