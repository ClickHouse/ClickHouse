-- Test that `shard_by_hash_input_batch_bytes` (batched input via ColumnsScatter::scatter)
-- produces per-key-identical GROUP BY results to the per-chunk default.
--
-- `max_block_size` is set small so each stream emits many input chunks, and the batched
-- thresholds below are chosen relative to that chunk size so the new batching paths in
-- `BufferedShardByHashTransform` are actually exercised (the previous version used the
-- default block size, so each stream produced a single chunk and only ever flushed at EOF):
--   shard_by_hash_input_batch_bytes = 16384      -- crossed after several chunks: multiple
--                                                   mid-stream flushes, multi-chunk batches,
--                                                   non-zero `pids_offset`
--   shard_by_hash_input_batch_bytes = 67108864   -- never crossed: a single EOF flush that
--                                                   still batches all pending chunks at once
--   shard_by_hash_input_batch_bytes = 0          -- per-chunk ColumnsScatter flush (baseline)
--
-- Each block compares `arraySort(groupArray((k, cnt)))` of the two batched configs against
-- the baseline and outputs 1 (both equal) or 0 (any differ). A bug that misroutes any row to
-- the wrong shard produces a different per-key count and trips the comparison to 0.

SET enable_sharding_aggregator = 1;
SET max_threads = 4;
SET max_block_size = 1000;

-- ── UInt64 key ────────────────────────────────────────────────────────────────
SELECT 'UInt64';
WITH
(
    SELECT arraySort(groupArray((k, cnt)))
    FROM (SELECT k, count() AS cnt FROM (SELECT number % 100 AS k FROM numbers_mt(50000)) GROUP BY k
          SETTINGS shard_by_hash_input_batch_bytes = 0)
) AS baseline
SELECT
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT number % 100 AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 16384)
    ) = baseline
)
AND
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT number % 100 AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 67108864)
    ) = baseline
);

-- ── String key ────────────────────────────────────────────────────────────────
SELECT 'String';
WITH
(
    SELECT arraySort(groupArray((k, cnt)))
    FROM (SELECT k, count() AS cnt FROM (SELECT toString(number % 50) AS k FROM numbers_mt(50000)) GROUP BY k
          SETTINGS shard_by_hash_input_batch_bytes = 0)
) AS baseline
SELECT
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT toString(number % 50) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 16384)
    ) = baseline
)
AND
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT toString(number % 50) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 67108864)
    ) = baseline
);

-- ── Nullable(UInt32) key ──────────────────────────────────────────────────────
SELECT 'NullableUInt32';
WITH
(
    SELECT arraySort(groupArray((k, cnt)))
    FROM (SELECT k, count() AS cnt FROM (SELECT if(number % 7 = 0, NULL, toUInt32(number % 30)) AS k FROM numbers_mt(50000)) GROUP BY k
          SETTINGS shard_by_hash_input_batch_bytes = 0)
) AS baseline
SELECT
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT if(number % 7 = 0, NULL, toUInt32(number % 30)) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 16384)
    ) = baseline
)
AND
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT if(number % 7 = 0, NULL, toUInt32(number % 30)) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 67108864)
    ) = baseline
);

-- ── Decimal64 key ─────────────────────────────────────────────────────────────
SELECT 'Decimal64';
WITH
(
    SELECT arraySort(groupArray((k, cnt)))
    FROM (SELECT k, count() AS cnt FROM (SELECT toDecimal64(number % 40, 2) AS k FROM numbers_mt(50000)) GROUP BY k
          SETTINGS shard_by_hash_input_batch_bytes = 0)
) AS baseline
SELECT
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT toDecimal64(number % 40, 2) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 16384)
    ) = baseline
)
AND
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT toDecimal64(number % 40, 2) AS k FROM numbers_mt(50000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 67108864)
    ) = baseline
);

-- ── Tuple(UInt32, String) key ─────────────────────────────────────────────────
SELECT 'TupleUInt32String';
WITH
(
    SELECT arraySort(groupArray((k, cnt)))
    FROM (SELECT k, count() AS cnt FROM (SELECT (toUInt32(number % 10), toString(number % 5)) AS k FROM numbers_mt(20000)) GROUP BY k
          SETTINGS shard_by_hash_input_batch_bytes = 0)
) AS baseline
SELECT
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT (toUInt32(number % 10), toString(number % 5)) AS k FROM numbers_mt(20000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 16384)
    ) = baseline
)
AND
(
    (
        SELECT arraySort(groupArray((k, cnt)))
        FROM (SELECT k, count() AS cnt FROM (SELECT (toUInt32(number % 10), toString(number % 5)) AS k FROM numbers_mt(20000)) GROUP BY k
              SETTINGS shard_by_hash_input_batch_bytes = 67108864)
    ) = baseline
);
