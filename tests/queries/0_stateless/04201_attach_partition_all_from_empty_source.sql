-- Test: exercises `StorageMergeTree::replacePartitionFrom` with `is_all=true` and an empty source
-- table — hits the `is_all` branch at src/Storages/StorageMergeTree.cpp:2563-2568, then
-- `getVisibleDataPartsVector` returns empty, the part-cloning loop is skipped, and the
-- `dst_parts.empty()` early-return at src/Storages/StorageMergeTree.cpp:2643-2644 fires.
-- The PR's own tests (00626_replace_partition_from_table.sql) only exercise non-empty source.

DROP TABLE IF EXISTS attach_all_empty_src;
DROP TABLE IF EXISTS attach_all_empty_dst;

CREATE TABLE attach_all_empty_src (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
CREATE TABLE attach_all_empty_dst (p UInt64, k String, d UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;

INSERT INTO attach_all_empty_dst VALUES (1, 'a', 100), (2, 'b', 200);

SELECT 'before';
SELECT count(), sum(d) FROM attach_all_empty_dst ORDER BY 1;

-- Source has zero parts: should be a no-op (no exception, dst unchanged)
ALTER TABLE attach_all_empty_dst ATTACH PARTITION ALL FROM attach_all_empty_src;

SELECT 'after';
SELECT count(), sum(d) FROM attach_all_empty_dst ORDER BY 1;

-- Self-attach with ALL: source equals destination, doubles the data via the new is_all branch
SYSTEM STOP MERGES attach_all_empty_dst;
ALTER TABLE attach_all_empty_dst ATTACH PARTITION ALL FROM attach_all_empty_dst;

SELECT 'after_self_attach';
SELECT count(), sum(d) FROM attach_all_empty_dst ORDER BY 1;

DROP TABLE attach_all_empty_src;
DROP TABLE attach_all_empty_dst;
