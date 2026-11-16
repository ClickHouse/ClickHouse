-- Tags: no-fasttest, no-ordinary-database, no-async-insert, no-object-storage, no-s3-storage
-- no-fasttest: UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
--
-- UNIQUE KEY: duplicate key values within one INSERT block are rejected with a
-- defined error at SST-write time (interim fail-closed stance: INSERT-time
-- dedup is not yet implemented), and no part is published. Covers both writer
-- paths: UK = sort prefix (sorted writer) and UK not a sort prefix (unsorted
-- writer, where the duplicates are only adjacent after the writer's UK sort).

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_dup_sorted;
CREATE TABLE uk_dup_sorted (k UInt64, v String)
ENGINE = MergeTree ORDER BY (k, v) UNIQUE KEY (k);

SELECT 'dup_sorted_rejected' AS step;
INSERT INTO uk_dup_sorted VALUES (1, 'a'), (1, 'b'); -- { serverError SUPPORT_IS_DISABLED }

-- Nothing was published: no rows, no active part.
SELECT count() FROM uk_dup_sorted;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'uk_dup_sorted' AND active;

-- The table stays usable: distinct keys insert fine.
SELECT 'distinct_sorted_ok' AS step;
INSERT INTO uk_dup_sorted VALUES (1, 'a'), (2, 'b');
SELECT count() FROM uk_dup_sorted;

DROP TABLE uk_dup_sorted;

-- UK not a sort prefix -> unsorted writer. The duplicate values are not
-- adjacent in the inserted block (sorted by id), only after the UK sort.
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
