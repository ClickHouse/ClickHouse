-- Tags: no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- `unique_key_max_encoded_size` admits or refuses an INSERT; it says nothing about rows already on
-- disk. A merge rebuilds the merged part's dense index from rows the table has already accepted, so
-- re-applying any cap there would strand a table whose keys were admitted under a roomier one.
--
-- DISCRIMINATING: read the cap in UniqueKeyDenseIndexOps::writeDenseIndexOnMerge or in
-- computeMergeLateKills' probe instead of passing numeric_limits<UInt64>::max(). Neither has a
-- per-query context -- `data.getContext()` is the global context and computeMergeLateKills takes no
-- Context at all -- so such a read lands on the DEFAULT of 256. Hence 400-byte keys: they clear the
-- INSERT cap set below, and trip a reintroduced read of the default. A session-level
-- `SET unique_key_max_encoded_size` would not, which is why this test does not use one.
--
-- no-object-storage / no-s3-storage: the dense index is opened by local filesystem path.

SET allow_experimental_unique_key = 1;

DROP TABLE IF EXISTS uk_encoded_size_merge;

CREATE TABLE uk_encoded_size_merge (k String, v UInt64)
ENGINE = MergeTree ORDER BY k UNIQUE KEY k
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES uk_encoded_size_merge;

-- Admitted under a cap that clears 400-byte keys, and above the 256 default.
INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number), number FROM numbers(50) SETTINGS unique_key_max_encoded_size = 4096;
INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number + 50), number FROM numbers(50) SETTINGS unique_key_max_encoded_size = 4096;
SELECT count(), count(DISTINCT k) FROM uk_encoded_size_merge;

-- The check still refuses a new row under a cap below it, so the test is not just proving the cap is dead.
INSERT INTO uk_encoded_size_merge SELECT repeat('k', 400) || toString(number + 500), number FROM numbers(1) SETTINGS unique_key_max_encoded_size = 8; -- { serverError BAD_ARGUMENTS }

-- The merge is not an admission decision, so it goes through over keys no cap would admit today.
SYSTEM START MERGES uk_encoded_size_merge;
OPTIMIZE TABLE uk_encoded_size_merge FINAL;

SELECT count(), count(DISTINCT k) FROM uk_encoded_size_merge;

DROP TABLE uk_encoded_size_merge;
