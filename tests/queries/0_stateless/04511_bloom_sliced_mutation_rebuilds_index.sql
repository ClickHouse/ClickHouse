-- Tags: no-random-settings, no-random-merge-tree-settings
-- Mutations that rewrite an indexed column must rebuild the bloom_sliced index. The metadata
-- uses the regular base `.idx` substream so the standard `hasSecondaryIndex` probe detects it:
-- wide parts must not keep stale hardlinked indexes, and compact parts must not end up with
-- orphan index files failing `CHECK TABLE`.
SET allow_experimental_bloom_sliced_index = 1;

-- Wide part: classical ALTER UPDATE of the indexed column.
DROP TABLE IF EXISTS bloom_sliced_rebuild_wide;

CREATE TABLE bloom_sliced_rebuild_wide
(
    id UInt64,
    text String,
    other String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO bloom_sliced_rebuild_wide
SELECT number, if(number = 42, 'needle present', 'filler line'), 'orig' FROM numbers(100);

ALTER TABLE bloom_sliced_rebuild_wide UPDATE text = 'needle appeared' WHERE id = 7 SETTINGS mutations_sync = 2;

SELECT '-- wide part, ALTER UPDATE of the indexed column: updated row is found';
SELECT id FROM bloom_sliced_rebuild_wide WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id FROM bloom_sliced_rebuild_wide WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
CHECK TABLE bloom_sliced_rebuild_wide SETTINGS check_query_single_value_result = 1;

SELECT '-- the rebuilt index prunes granules and the hint is active';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_rebuild_wide WHERE hasToken(text, 'needle'))
WHERE explain LIKE '%Granules: 2/10%';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_rebuild_wide WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

-- An unrelated mutation must not corrupt the index either.
ALTER TABLE bloom_sliced_rebuild_wide UPDATE other = 'mutated' WHERE id = 3 SETTINGS mutations_sync = 2;

SELECT '-- wide part, unrelated mutation: index stays consistent';
SELECT id FROM bloom_sliced_rebuild_wide WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
CHECK TABLE bloom_sliced_rebuild_wide SETTINGS check_query_single_value_result = 1;

DROP TABLE bloom_sliced_rebuild_wide;

-- Note: rebuilds triggered by materializing patch parts (lightweight UPDATE + APPLY PATCHES, or
-- an unrelated mutation transitively applying a pending patch) additionally require the upstream
-- MutationsInterpreter fix that feeds read-for-patch columns into the skip-index rebuild
-- predicate (a general bug that equally affects e.g. the `text` index). They are covered by the
-- regression test shipped with that fix and are intentionally not asserted here.

-- Compact part: mutations must not leave orphan index files.
DROP TABLE IF EXISTS bloom_sliced_rebuild_compact;

CREATE TABLE bloom_sliced_rebuild_compact
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_rebuild_compact
SELECT number, if(number = 42, 'needle present', 'filler line') FROM numbers(100);

ALTER TABLE bloom_sliced_rebuild_compact UPDATE text = 'needle appeared' WHERE id = 7 SETTINGS mutations_sync = 2;

SELECT '-- compact part, ALTER UPDATE of the indexed column: updated row is found, part is consistent';
SELECT id FROM bloom_sliced_rebuild_compact WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
CHECK TABLE bloom_sliced_rebuild_compact SETTINGS check_query_single_value_result = 1;

SELECT '-- the rebuilt index prunes granules';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_rebuild_compact WHERE hasToken(text, 'needle'))
WHERE explain LIKE '%Granules: 2/10%';

DELETE FROM bloom_sliced_rebuild_compact WHERE id = 42;

SELECT '-- compact part, lightweight delete: results correct, part is consistent';
SELECT id FROM bloom_sliced_rebuild_compact WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
CHECK TABLE bloom_sliced_rebuild_compact SETTINGS check_query_single_value_result = 1;

DROP TABLE bloom_sliced_rebuild_compact;
