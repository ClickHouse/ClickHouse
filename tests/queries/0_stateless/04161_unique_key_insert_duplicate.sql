-- Tags: no-fasttest, no-ordinary-database
-- UNIQUE KEY: an INSERT block with a duplicate key is rejected and publishes nothing.
--   1. sorted writer: UK is a sort prefix, duplicates are adjacent
--   2. unsorted writer: UK is not a sort prefix, duplicates meet only after the UK sort
-- no-fasttest: a UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.

SET enable_unique_key = 1;

-- 1. sorted writer: red if the dense-index writer accepts a repeated key.
DROP TABLE IF EXISTS uk_dup_sorted;
CREATE TABLE uk_dup_sorted (k UInt64, v String)
ENGINE = MergeTree ORDER BY (k, v) UNIQUE KEY (k);

SELECT 'dup_sorted_rejected' AS step;
INSERT INTO uk_dup_sorted VALUES (1, 'a'), (1, 'b'); -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM uk_dup_sorted;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_dup_sorted' AND active;

SELECT 'distinct_sorted_ok' AS step;
INSERT INTO uk_dup_sorted VALUES (1, 'a'), (2, 'b');
SELECT count() FROM uk_dup_sorted;

DROP TABLE uk_dup_sorted;

-- 2. unsorted writer: red if the same check is skipped when the writer sorts the block by the key.
DROP TABLE IF EXISTS uk_dup_unsorted;
CREATE TABLE uk_dup_unsorted (id UInt64, k UInt64)
ENGINE = MergeTree ORDER BY (id) UNIQUE KEY (k);

SELECT 'dup_unsorted_rejected' AS step;
INSERT INTO uk_dup_unsorted VALUES (1, 5), (2, 3), (3, 5); -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM uk_dup_unsorted;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_dup_unsorted' AND active;

SELECT 'distinct_unsorted_ok' AS step;
INSERT INTO uk_dup_unsorted VALUES (1, 5), (2, 3), (3, 6);
SELECT count() FROM uk_dup_unsorted;

DROP TABLE uk_dup_unsorted;
