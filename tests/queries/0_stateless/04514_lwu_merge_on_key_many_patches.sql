-- Tags: no-replicated-database
-- no-replicated-database: SYSTEM STOP MERGES is not replicated.

-- Many patch parts with the same set of updated columns force the merge pass in
-- applyPatchesMergeOnKey through the heap of cursors (more cursors than the
-- small-count linear-scan specialization handles).

SET allow_experimental_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_many_patches SYNC;

CREATE TABLE t_lwu_many_patches (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_many_patches SELECT number, 0 FROM numbers(100000);
SYSTEM STOP MERGES t_lwu_many_patches;

UPDATE t_lwu_many_patches SET v = 1 WHERE id % 2 = 0;
UPDATE t_lwu_many_patches SET v = 2 WHERE id % 3 = 0;
UPDATE t_lwu_many_patches SET v = 3 WHERE id % 5 = 0;
UPDATE t_lwu_many_patches SET v = 4 WHERE id % 7 = 0;
UPDATE t_lwu_many_patches SET v = 5 WHERE id % 11 = 0;
UPDATE t_lwu_many_patches SET v = 6 WHERE id % 13 = 0;
UPDATE t_lwu_many_patches SET v = 7 WHERE id % 17 = 0;
UPDATE t_lwu_many_patches SET v = 8 WHERE id % 19 = 0;
UPDATE t_lwu_many_patches SET v = 9 WHERE id % 23 = 0;
UPDATE t_lwu_many_patches SET v = 10 WHERE id % 29 = 0;
UPDATE t_lwu_many_patches SET v = 11 WHERE id % 31 = 0;
UPDATE t_lwu_many_patches SET v = 12 WHERE id % 37 = 0;

SELECT countIf(startsWith(name, 'patch'))
FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_many_patches' AND active;

-- Later updates win on overlapping rows: the expected value is the highest
-- update index whose modulus divides id.
SELECT countIf(v != multiIf(
    id % 37 = 0, 12,
    id % 31 = 0, 11,
    id % 29 = 0, 10,
    id % 23 = 0, 9,
    id % 19 = 0, 8,
    id % 17 = 0, 7,
    id % 13 = 0, 6,
    id % 11 = 0, 5,
    id % 7 = 0, 4,
    id % 5 = 0, 3,
    id % 3 = 0, 2,
    id % 2 = 0, 1,
    0)) AS mismatches
FROM t_lwu_many_patches;

DROP TABLE t_lwu_many_patches SYNC;
