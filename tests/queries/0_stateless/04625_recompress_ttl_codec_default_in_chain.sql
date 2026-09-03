-- Tags: no-random-settings

-- The `Default` alias inside a `TTL ... RECOMPRESS` codec chain (e.g. `CODEC(Delta, Default)`)
-- must resolve through the normal default selection — the `default_compression_codec` setting,
-- then the server `<compression>` selector — the same way a bare `CODEC(Default)` does, instead
-- of silently substituting the factory's hardcoded fallback (`LZ4`).

DROP TABLE IF EXISTS t_recompress_default_in_chain;

CREATE TABLE t_recompress_default_in_chain
(
    dt DateTime,
    x UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(Delta, Default)
SETTINGS default_compression_codec = 'ZSTD(3)', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_recompress_default_in_chain;

-- Data inserted with an already-expired `RECOMPRESS` TTL.
INSERT INTO t_recompress_default_in_chain SELECT now() - INTERVAL 1 DAY, number FROM numbers(100000);

-- Before recompression the configured `default_compression_codec` applies.
SELECT 'before merge';
SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_default_in_chain' AND active;

SYSTEM START TTL MERGES t_recompress_default_in_chain;
OPTIMIZE TABLE t_recompress_default_in_chain FINAL;

-- After the recompression merge the `Default` entry in the chain must have resolved to the
-- table's `default_compression_codec` (`ZSTD(3)`), not to the factory fallback (`LZ4`).
SELECT 'after recompress';
SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_recompress_default_in_chain' AND active;

SELECT count(), sum(x) FROM t_recompress_default_in_chain;

DROP TABLE t_recompress_default_in_chain;
