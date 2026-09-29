-- Keep mutations pending so the reader applies them before PREWHERE.
SET mutations_sync = 0;
SET apply_mutations_on_fly = 1;
SET optimize_move_to_prewhere = 0;
SET optimize_functions_to_subcolumns = 1;
SET optimize_string_size_subcolumn_with_full_read = 1;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS test_string_filter_mutations SYNC;
CREATE TABLE test_string_filter_mutations
(
    id UInt64,
    part UInt8,
    s String
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    serialization_info_version = 'with_types',
    string_serialization_version = 'single_stream',
    ratio_of_defaults_for_sparse_serialization = 0;

SYSTEM STOP MERGES test_string_filter_mutations;
INSERT INTO test_string_filter_mutations VALUES
    (0, 0, 'old'), (1, 0, ''), (2, 0, 'keep'), (3, 0, 'grow');

ALTER TABLE test_string_filter_mutations MODIFY SETTING
    string_serialization_version = 'with_size_stream';
INSERT INTO test_string_filter_mutations VALUES
    (4, 1, 'old'), (5, 1, ''), (6, 1, 'keep'), (7, 1, 'grow');

-- Each layout has a nonempty-to-empty update, an empty-to-nonempty update,
-- and two unchanged rows. A second update must use the first update's result.
ALTER TABLE test_string_filter_mutations
    UPDATE s = if(id % 4 = 0, '', concat(s, '!')) WHERE id % 4 < 2;
ALTER TABLE test_string_filter_mutations
    UPDATE s = concat(s, 'xx') WHERE id % 4 = 1;

SELECT 'full-String control sees pending updates';
SELECT id, s FROM test_string_filter_mutations PREWHERE empty(s) ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

-- The injected legacy size must not be carried unchanged through the UPDATE.
SELECT 'rewritten empty filter sees updated sizes';
SELECT id, s FROM test_string_filter_mutations PREWHERE empty(s) ORDER BY id;

SELECT 'rewritten nonempty filter sees updated sizes';
SELECT id, s FROM test_string_filter_mutations PREWHERE notEmpty(s) ORDER BY id;

SELECT 'rewritten length filter sees chained updates';
SELECT id, s FROM test_string_filter_mutations PREWHERE length(s) = 3 ORDER BY id;

-- Explicit subcolumns are independent of the analyzer opt-in. In particular,
-- this size-only query has mutation steps but no PREWHERE steps.
SELECT 'explicit size-only read without analyzer rewrites';
SELECT s.size FROM test_string_filter_mutations ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, optimize_string_size_subcolumn_with_full_read = 0;

SELECT 'explicit size filter with a later String consumer';
SELECT id, s, s.size FROM test_string_filter_mutations
PREWHERE s.size > 0 AND id > 0 AND position(s, '!') = 1
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, optimize_string_size_subcolumn_with_full_read = 0;

SELECT 'explicit size filter with a single PREWHERE step';
SELECT id, s, s.size FROM test_string_filter_mutations
PREWHERE s.size = 0
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, enable_multiple_prewhere_read_steps = 0;

-- Verify that the original on-disk values really are still present.
SELECT 'mutations remain pending';
SELECT id, s FROM test_string_filter_mutations ORDER BY id
SETTINGS apply_mutations_on_fly = 0, optimize_functions_to_subcolumns = 0;

DROP TABLE test_string_filter_mutations SYNC;
