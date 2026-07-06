-- Regression test for in-place modification of the discriminators column kept in the
-- deserialization state of a Variant element (and its null map) while it is still shared with
-- the state of another subcolumn of the same Variant through the substreams cache. The
-- discontiguous key ranges make one block span several `readRows` calls, so later ranges append
-- to the shared column. Covers both BASIC and COMPACT discriminator serialization modes. The
-- corruption is only observable as UAF under ASan; the assertions below pin the correct results.

DROP TABLE IF EXISTS t_variant_discr_basic;
DROP TABLE IF EXISTS t_variant_discr_compact;

CREATE TABLE t_variant_discr_basic (k UInt64, v Variant(String, UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32,
         min_bytes_for_wide_part = 0,
         use_compact_variant_discriminators_serialization = 0;

-- Non-numeric strings: CAST from String to Variant parses numeric strings into UInt64.
INSERT INTO t_variant_discr_basic
    SELECT number, CAST('s' || toString(number), 'Variant(String, UInt64)') FROM numbers(20000) WHERE number % 3 = 0;
INSERT INTO t_variant_discr_basic
    SELECT number, CAST(number, 'Variant(String, UInt64)') FROM numbers(20000) WHERE number % 3 = 1;
INSERT INTO t_variant_discr_basic
    SELECT number, NULL FROM numbers(20000) WHERE number % 3 = 2;

OPTIMIZE TABLE t_variant_discr_basic FINAL;

CREATE TABLE t_variant_discr_compact (k UInt64, v Variant(String, UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32,
         min_bytes_for_wide_part = 0,
         use_compact_variant_discriminators_serialization = 1;

INSERT INTO t_variant_discr_compact SELECT * FROM t_variant_discr_basic;

OPTIMIZE TABLE t_variant_discr_compact FINAL;

-- Two element subcolumns share the discriminators through the cache.
SELECT countIf(startsWith(v.String, 's')), sum(v.UInt64), count()
FROM t_variant_discr_basic
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

-- Null map of one element plus another element.
SELECT sum(v.String.null), sum(v.UInt64), count()
FROM t_variant_discr_basic
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

SELECT countIf(startsWith(v.String, 's')), sum(v.UInt64), count()
FROM t_variant_discr_compact
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

SELECT sum(v.String.null), sum(v.UInt64), count()
FROM t_variant_discr_compact
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

DROP TABLE t_variant_discr_basic;
DROP TABLE t_variant_discr_compact;
