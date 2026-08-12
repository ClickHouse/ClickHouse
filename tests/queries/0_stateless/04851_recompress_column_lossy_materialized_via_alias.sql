-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` must see a stored `MATERIALIZED`
-- dependent that reaches the recompressed column through an `ALIAS` helper: `a ALIAS x * 2,
-- m MATERIALIZED a + 1` stores `m` computed from `x`, so a lossy recompression of `x` would leave
-- `m` describing the pre-recompression values. The dependency analysis expands `ALIAS` chains the
-- same way it expands `EPHEMERAL` ones.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;

-- A stored MATERIALIZED column depends on the lossy column through an ALIAS: rejected.
DROP TABLE IF EXISTS t_recompress_lossy_materialized_alias;
CREATE TABLE t_recompress_lossy_materialized_alias
(
    key UInt64,
    x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a Float64 ALIAS x * 2,
    m Float64 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized_alias (key, x) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized_alias RECOMPRESS COLUMN x; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_materialized_alias;

-- The same through a chain of two ALIAS columns.
DROP TABLE IF EXISTS t_recompress_lossy_materialized_alias_chain;
CREATE TABLE t_recompress_lossy_materialized_alias_chain
(
    key UInt64,
    x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a Float64 ALIAS x * 2,
    b Float64 ALIAS a + 1,
    m Float64 MATERIALIZED b * 3
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized_alias_chain (key, x) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized_alias_chain RECOMPRESS COLUMN x; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_materialized_alias_chain;

-- A MATERIALIZED column reaching a different column through an ALIAS does not block the
-- recompression, and the presence of the ALIAS helper alone does not break the analysis.
DROP TABLE IF EXISTS t_recompress_lossy_materialized_alias_other;
CREATE TABLE t_recompress_lossy_materialized_alias_other
(
    key UInt64,
    x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a UInt64 ALIAS key * 2,
    m UInt64 MATERIALIZED a + 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_materialized_alias_other (key, x) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_materialized_alias_other RECOMPRESS COLUMN x;
SELECT 'materialized via alias over another column', count(), countIf(m = key * 2 + 1) FROM t_recompress_lossy_materialized_alias_other;

DROP TABLE t_recompress_lossy_materialized_alias_other;

-- An ALIAS column itself depending on the lossy column does not block the recompression: it is
-- not stored, so it is recomputed from the post-recompression values at read time.
DROP TABLE IF EXISTS t_recompress_lossy_alias_only;
CREATE TABLE t_recompress_lossy_alias_only
(
    key UInt64,
    x Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    a Float64 ALIAS x * 2
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_alias_only (key, x) SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_alias_only RECOMPRESS COLUMN x;
SELECT 'alias only', count(), countIf(a = x * 2) FROM t_recompress_lossy_alias_only;

DROP TABLE t_recompress_lossy_alias_only;
