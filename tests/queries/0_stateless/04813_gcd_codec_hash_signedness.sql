SET optimize_trivial_insert_select = 0;

-------------------------------------------------------------------------------------------
-- Regression test for a possible implementation error where the same compressor instance is
-- shared between signed and unsigned `CODEC(GCD)` columns, even though they must be compressed
-- differently.
--
-- Per-column compressed/uncompressed bytes aren't available for Compact parts, so this test
-- compares whole-part sizes instead: writing two GCD columns into one part should be (near)
-- additive versus writing each column alone.

DROP TABLE IF EXISTS t_gcd_hash_combined;
DROP TABLE IF EXISTS t_gcd_hash_int64_only;
DROP TABLE IF EXISTS t_gcd_hash_uint64_only;

-- Both columns are 8-byte, so gcd_bytes_size alone can't distinguish them.
CREATE TABLE t_gcd_hash_combined
(
    col_int64  Int64  CODEC(GCD, ZSTD),
    col_uint64 UInt64 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
/* Force a Compact part so both columns can share a compressed stream. */
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

/* Both columns are populated by a single INSERT so they land in the same part and can share
   a compressed stream.
   Int64: multiples of 10^15 in [-1000, 1000] * 10^15, so a signed-aware `gcd` of ~10^15 is
   only found when the magnitude is computed from the sign-corrected value.
   UInt64: multiples of 10^6, sampled from the upper half of the UInt64 range (top bit set),
   so a value wrongly treated as signed would flip to its two's-complement magnitude and lose
   the factor-of-5^6 part of the divisor, collapsing the found `gcd` from 10^6 to 64.
   `intHash64` gives deterministic pseudo-random values, so a failure is reproducible and the
   single-column tables hold exactly the same values as the combined one. */
INSERT INTO t_gcd_hash_combined
SELECT
    (toInt64(intHash64(number) % 2001) - 1000) * 1000000000000000,
    (9223372036855 + (intHash64(number + 100000) % 9223372036854)) * 1000000
FROM numbers(100000);

INSERT INTO t_gcd_hash_int64_only
SELECT (toInt64(intHash64(number) % 2001) - 1000) * 1000000000000000
FROM numbers(100000);

INSERT INTO t_gcd_hash_uint64_only
SELECT (9223372036855 + (intHash64(number + 100000) % 9223372036854)) * 1000000
FROM numbers(100000);

SELECT DISTINCT part_type
FROM system.parts
WHERE `database` = currentDatabase()
    AND `table` IN ('t_gcd_hash_combined', 't_gcd_hash_int64_only', 't_gcd_hash_uint64_only')
    AND active;

WITH
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_combined' AND active) AS combined_bytes,
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_int64_only' AND active) AS int64_only_bytes,
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE `database` = currentDatabase() AND `table` = 't_gcd_hash_uint64_only' AND active) AS uint64_only_bytes
SELECT combined_bytes <= 1.05 * (int64_only_bytes + uint64_only_bytes) AS gcd_hash_no_stream_collision_signedness;

DROP TABLE t_gcd_hash_combined;
DROP TABLE t_gcd_hash_int64_only;
DROP TABLE t_gcd_hash_uint64_only;
