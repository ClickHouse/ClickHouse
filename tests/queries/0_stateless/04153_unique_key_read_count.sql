-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY read-path: trivial count() goes through the reconciled
-- totalRows / totalRowsByPartitionPredicate seam (per-partition snapshot,
-- subtract dead rows). Write-driven dedup/DELETE is out of scope on this
-- branch, so with no deletions count() == raw inserted rows. Bitmap-driven
-- supersession counts + observability defer to the first writer PR.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_count;
DROP TABLE IF EXISTS uk_count_part;
DROP TABLE IF EXISTS plain_count;

-- 1. count() == 0 on an empty UK table. Exercises totalRows UK branch:
--    hasUniqueKey() -> snapshot parts (none) -> getDeadRowsForUniqueKey == 0.
CREATE TABLE uk_count (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SELECT count() FROM uk_count;

-- 2. count() after a sync INSERT == raw rows (no deletions visible).
INSERT INTO uk_count VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT count() FROM uk_count;

-- 3. A full-scan aggregate (not the trivial-count shortcut) agrees.
SELECT sum(id) FROM uk_count SETTINGS optimize_trivial_count_query = 0;

-- 4. Partitioned UK table: totalRowsByPartitionPredicate UK branch per
--    partition. Empty -> 0; after insert -> raw rows; filtered count agrees.
CREATE TABLE uk_count_part (id UInt64, p UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
PARTITION BY p;

SELECT count() FROM uk_count_part;
INSERT INTO uk_count_part VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c');
SELECT count() FROM uk_count_part;
SELECT count() FROM uk_count_part WHERE p = 10;

-- 5. Plain MergeTree count() is unaffected by the UK branch (fast path).
CREATE TABLE plain_count (id UInt64) ENGINE = MergeTree ORDER BY id;
SELECT count() FROM plain_count;
INSERT INTO plain_count VALUES (1), (2);
SELECT count() FROM plain_count;

DROP TABLE uk_count;
DROP TABLE uk_count_part;
DROP TABLE plain_count;
