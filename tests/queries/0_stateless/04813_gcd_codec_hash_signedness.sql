SET optimize_trivial_insert_select = 0;

-------------------------------------------------------------------------------------------
-- `CompressionCodecGCD::updateHash` must include `gcd_bytes_size` and `is_signed_type`,
-- because `MergeTreeDataPartWriterCompact` keys `CompressedStream` reuse on `codec->getHash()`
-- (see `addStreams` in `MergeTreeDataPartWriterCompact.cpp`). Without that, an `Int64
-- CODEC(GCD)` column and a `UInt64 CODEC(GCD)` column written into the same Compact part
-- could hash-collide and share one `CompressedStream`, so one of the two columns would
-- silently adopt the *other* column's sign convention. This does not break the round-trip
-- (multiplying the stored quotient by the found `gcd` modulo 2^64 reconstructs the original
-- bits regardless of sign convention), but it can pick a far smaller `gcd` for the affected
-- column, degrading its compression ratio.
--
-- Per-column compressed/uncompressed byte accounting (`system.columns`) is not populated for
-- Compact parts (`MergeTreeDataPartCompact::calculateEachColumnSizes` only fills the total),
-- so this test compares whole-part sizes instead: writing the `Int64` and `UInt64` GCD
-- columns into one Compact part should be (near) additive versus writing each column alone.
-- A `CompressedStream` collision inflates the combined size well above the sum of the parts.

DROP TABLE IF EXISTS t_gcd_hash_combined;
DROP TABLE IF EXISTS t_gcd_hash_int64_only;
DROP TABLE IF EXISTS t_gcd_hash_uint64_only;

CREATE TABLE t_gcd_hash_combined
(
    col_int64  Int64  CODEC(GCD, ZSTD),
    col_uint64 UInt64 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
/* Force a Compact part so both columns are written by the same MergeTreeDataPartWriterCompact instance. */
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;

CREATE TABLE t_gcd_hash_int64_only
(
    col_int64 Int64 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;

CREATE TABLE t_gcd_hash_uint64_only
(
    col_uint64 UInt64 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;

/* Both columns are populated by a single INSERT so they land in the same part, written by
   the same MergeTreeDataPartWriterCompact instance (and thus share one `streams_by_codec` map).
   Int64: multiples of 10^15 in [-1000, 1000] * 10^15, so a signed-aware `gcd` of ~10^15 is
   only found when the magnitude is computed from the sign-corrected value.
   UInt64: multiples of 10^6, sampled from the upper half of the UInt64 range (top bit set),
   so a value wrongly treated as signed would flip to its two's-complement magnitude and lose
   the factor-of-5^6 part of the divisor, collapsing the found `gcd` from 10^6 to 64. */
INSERT INTO t_gcd_hash_combined
SELECT
    ((reinterpretAsInt64(rand64()) % 2001) - 1000) * 1000000000000000,
    (9223372036855 + (rand64() % 9223372036854)) * 1000000
FROM numbers(100000);

INSERT INTO t_gcd_hash_int64_only
SELECT ((reinterpretAsInt64(rand64()) % 2001) - 1000) * 1000000000000000
FROM numbers(100000);

INSERT INTO t_gcd_hash_uint64_only
SELECT (9223372036855 + (rand64() % 9223372036854)) * 1000000
FROM numbers(100000);

SELECT DISTINCT part_type
FROM system.parts
WHERE `database` = currentDatabase() AND `table` IN ('t_gcd_hash_combined', 't_gcd_hash_int64_only', 't_gcd_hash_uint64_only') AND active;

WITH
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_combined' AND active) AS combined_bytes,
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_int64_only' AND active) AS int64_only_bytes,
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_uint64_only' AND active) AS uint64_only_bytes
SELECT combined_bytes <= 1.05 * (int64_only_bytes + uint64_only_bytes) AS gcd_hash_no_stream_collision;

DROP TABLE t_gcd_hash_combined;
DROP TABLE t_gcd_hash_int64_only;
DROP TABLE t_gcd_hash_uint64_only;
