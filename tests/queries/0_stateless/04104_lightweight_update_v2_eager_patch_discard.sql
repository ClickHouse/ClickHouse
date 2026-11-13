-- Regression test: `MergeTreePatchReaderMergeOnKey::readPatches` must not retain patch blocks
-- whose max sort-key is already below the current main block's min sort-key.
--
-- Before the fix, when the caller's deque was drained (main cursor advanced past everything),
-- the "catch-up" phase read every range whose max was still below `main_max` and pushed all
-- of them into `results`. Each block is ~8 marks of patch data. With a large main cursor and a
-- from-scratch patch reader — the usual situation when the patch-covered region lies at the
-- end of the main part's sort-key range — that accumulation ran across *most* of the patch.
-- In a run with 5 patches of ~1 MiB uncompressed each, a single `SELECT count()` with
-- `max_threads = 1` peaked at ~258 MiB (close to the full union of all patch blocks); the
-- user's 96-patch, 200 M-row scenario OOM'd at 60 GiB.
--
-- The fix: inline-discard each newly-read block for which `needOldPatch` is already false
-- (block's max sort-key < main's min sort-key). It cannot contribute to any current or later
-- main row and `MergeTreeReadersChain::readPatches` would evict it anyway on the next iteration.
-- We keep exactly the blocks that are required to cover the current main block plus one read
-- ahead — memory stays O(#patches), not O(patch_size · #patches).
--
-- The test creates a scenario where catch-up is exercised: multiple non-interleaved patch parts
-- and a `SELECT` that walks the full main part. We can't rely on absolute memory numbers in CI,
-- but peak memory for this shape should be single-digit MiB. Use `max_memory_usage` as the
-- assertion: if the regression reappears the query will exceed the tight budget.

DROP TABLE IF EXISTS t_v2_eager_discard;

CREATE TABLE t_v2_eager_discard (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1,
         enable_block_offset_column = 1,
         apply_patches_on_merge = 0,
         patch_parts_version = 'v2',
         index_granularity = 1024;

-- Five separate inserts → five main parts covering disjoint `a` ranges. One DELETE per part
-- creates one patch per part; the resulting `SELECT` reads main part N with a patch reader
-- whose sort-key range starts well below part N's sort-key range — this is the shape that
-- previously pinned the whole patch in memory during the `last_read_patch = nullptr` catch-up.
SYSTEM STOP MERGES t_v2_eager_discard;

INSERT INTO t_v2_eager_discard SELECT number FROM numbers(0,        200000);
INSERT INTO t_v2_eager_discard SELECT number FROM numbers(200000,   200000);
INSERT INTO t_v2_eager_discard SELECT number FROM numbers(400000,   200000);
INSERT INTO t_v2_eager_discard SELECT number FROM numbers(600000,   200000);
INSERT INTO t_v2_eager_discard SELECT number FROM numbers(800000,   200000);

SET lightweight_delete_mode = 'lightweight_update_force';
SET max_threads = 1;

-- The budget below measures the patch reader's own memory. On object storage the prefetched
-- read pool charges its read buffers to the same query - they are bounded by
-- `filesystem_prefetch_max_memory_usage`, which the test randomizer draws from 32/64/128 MiB -
-- and that has nothing to do with what this test asserts. On an `amd_msan, s3 storage` run with
-- a randomized `allow_prefetched_read_pool_for_remote_filesystem = 1` the first query peaked at
-- 62.05 MiB against the 61.04 MiB budget, and the failure reproduced 6 times out of 6 with the
-- same settings while passing without randomization. Pin the setting off, as the randomizer
-- itself already does for debug builds.
SET allow_prefetched_read_pool_for_remote_filesystem = 0;

DELETE FROM t_v2_eager_discard WHERE (a % 2) = 0;

-- Tight budget. Enough patch metadata and one working block per patch fit comfortably;
-- the pre-fix behavior (pinning tens of MiB per patch) would blow through this.
SELECT count() FROM t_v2_eager_discard SETTINGS max_memory_usage = '64M';
SELECT count() FROM t_v2_eager_discard WHERE a > 499000 SETTINGS max_memory_usage = '64M';
SELECT min(a), max(a) FROM t_v2_eager_discard SETTINGS max_memory_usage = '64M';

DROP TABLE t_v2_eager_discard;
