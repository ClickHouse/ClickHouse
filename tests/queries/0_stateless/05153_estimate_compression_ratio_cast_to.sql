-- Tests for the `cast_to` parameter of `estimateCompressionRatio`: simulates compressing the column
-- as if it had a different type. Needed because the aggregate function factory strips `LowCardinality`
-- from argument types before construction, so `estimateCompressionRatio(toLowCardinality(x))` cannot
-- see a `LowCardinality` column. The ratio always divides by the column's own current uncompressed
-- size, not the cast-to representation's, so ratios stay comparable across different `cast_to` choices.

-- `cast_to` parses in any position alongside `codec` and `block_size_bytes`, in any order. Dispatch
-- happens at construction time, so an empty input suffices and gives the deterministic `0` from
-- 03363_estimate_compression_ratio_validation.sql.
SELECT estimateCompressionRatio('LZ4', 1048576, 'UInt64')(number)
FROM numbers(0)
;

SELECT estimateCompressionRatio('UInt64', 'LZ4', 1048576)(number)
FROM numbers(0)
;

SELECT estimateCompressionRatio(1048576, 'UInt64', 'LZ4')(number)
FROM numbers(0)
;

SELECT estimateCompressionRatio('LZ4', 1048576, 'LowCardinality(String)')(toString(number))
FROM numbers(0)
;

-- Compares String vs LowCardinality(String) compression. Ground truth is a real LowCardinality(String)
-- column's on-disk size, not `estimateCompressionRatio(...)(str_lc)`: the same factory-level stripping
-- would apply to that call too.
DROP TABLE IF EXISTS t_cast_to_low_cardinality;

CREATE TABLE t_cast_to_low_cardinality
(
    `str` String,
    `str_lc` LowCardinality(String) CODEC(LZ4)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 65536, index_granularity = 8192
;

INSERT INTO t_cast_to_low_cardinality SELECT
    concat('category_', toString(number % 50)),
    concat('category_', toString(number % 50))
FROM numbers(100000)
;

OPTIMIZE TABLE t_cast_to_low_cardinality FINAL;

-- Predicted compressed size (real uncompressed / ratio) should be close to str_lc's real on-disk
-- compressed size. Not exact, due to the same imprecision as other tests in 03364_estimate_compression_ratio.sh
-- and additionally the fact that LowCardinality handled per block rather than per column in the estimator.
SELECT (abs(on_disk_compressed_bytes - simulated_compressed_bytes) / on_disk_compressed_bytes) < 0.45
FROM
(
    SELECT
        (
            SELECT column_data_compressed_bytes
            FROM system.parts_columns
            WHERE (database = currentDatabase()) AND (`table` = 't_cast_to_low_cardinality') AND active AND (column = 'str_lc')
        ) AS on_disk_compressed_bytes,
        toUInt64(round((
            SELECT column_data_uncompressed_bytes
            FROM system.parts_columns
            WHERE (database = currentDatabase()) AND (`table` = 't_cast_to_low_cardinality') AND active AND (column = 'str')
        ) / (
            SELECT estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(str)
            FROM t_cast_to_low_cardinality
            SETTINGS max_block_size = 65536
        ))) AS simulated_compressed_bytes
)
;

-- The LowCardinality(String) estimate must be markedly higher than the plain-String one: both divide
-- by the same uncompressed size, so a higher ratio means a smaller predicted compressed size, and
-- dictionary-encoding 50 distinct values compresses much better than the raw repeated strings.
SELECT estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(str) > (estimateCompressionRatio('LZ4', 65536)(str) * 1.5)
FROM t_cast_to_low_cardinality
SETTINGS max_block_size = 65536
;

DROP TABLE t_cast_to_low_cardinality;

-- CAST maps an IPv4 address into the IPv6 address space, adding a constant 12-byte prefix. Whether
-- that's worth it is codec-dependent: `ZSTD` compresses the constant prefix away and wins; `LZ4`
-- doesn't, and ends up slightly larger instead. `ZSTD` is used here as the case that actually helps.
SELECT estimateCompressionRatio('ZSTD', 'IPv6')(toIPv4(number)) > (estimateCompressionRatio('ZSTD')(toIPv4(number)) * 1.4)
FROM numbers(100000)
;

-- Casting to a structurally incompatible type fails (only once real data reaches the cast).
SELECT estimateCompressionRatio('LZ4', 'Tuple(Int64, Int64)')(number)
FROM numbers(10)
; -- { serverError TYPE_MISMATCH }

-- Parameter validation: at most one type parameter.
SELECT estimateCompressionRatio('UInt64', 'Int64')(number)
FROM numbers(1)
; -- { serverError BAD_QUERY_PARAMETER }

-- At most three parameters in total.
SELECT estimateCompressionRatio('LZ4', 65536, 'UInt64', 'Int64')(number)
FROM numbers(1)
; -- { serverError UNKNOWN_QUERY_PARAMETER }

-- A String that resolves to neither a valid codec nor a valid type name fails as an unknown codec
-- (the `cast_to`/`codec` disambiguation falls back to treating it as a codec).
SELECT estimateCompressionRatio('NotACodecOrType')(number)
FROM numbers(1); -- { serverError UNKNOWN_CODEC }

-- `cast_to` also works through the row-at-a-time `add()` path used by window functions, not just the
-- bulk path above. Growing value lengths move the ratio row to row, checked via distinct-value count
-- (matching 04267_estimate_compression_ratio_window_accumulation.sql) rather than exact values.
SELECT uniq(ratio) = 5
FROM
(
    SELECT estimateCompressionRatio('LZ4', 1048576, 'LowCardinality(String)')(repeat(toString(number % 3), (number + 1) * 50)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ratio
    FROM numbers(5)
)
;