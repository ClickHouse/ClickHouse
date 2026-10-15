-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- `isNull` / `isNotNull` Exact must not use lazy token cursors (empty token list → all zeros),
-- and must clip part-wide identity postings to the current read window.
-- `Nullable(FixedString)` is defined-capable and must match the scan path.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET text_index_posting_list_apply_mode = 'lazy';

DROP TABLE IF EXISTS text_index_defined_bitpacking;
DROP TABLE IF EXISTS text_index_defined_fixed_string;

CREATE TABLE text_index_defined_bitpacking
(
    id UInt32,
    value Nullable(String),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_bitpacking;

INSERT INTO text_index_defined_bitpacking VALUES
    (0, 'alpha'), (1, 'alpha'), (2, 'alpha'), (3, 'alpha'),
    (4, 'alpha'), (5, 'alpha'), (6, 'alpha'), (7, 'alpha'),
    (8, NULL), (9, NULL), (10, NULL), (11, NULL),
    (12, NULL), (13, NULL), (14, NULL), (15, NULL),
    (16, 'beta');

SELECT 'bitpacking isNull indexed';
SELECT id FROM text_index_defined_bitpacking WHERE isNull(value) ORDER BY id;
SELECT 'bitpacking isNull scan';
SELECT id FROM text_index_defined_bitpacking WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'bitpacking isNotNull indexed';
SELECT id FROM text_index_defined_bitpacking WHERE isNotNull(value) ORDER BY id;
SELECT 'bitpacking isNotNull scan';
SELECT id FROM text_index_defined_bitpacking WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- The tail mark starts at row 16. Identity still contains rows 0..7; filling without a
-- window clip would write those ids past a one-row column.
SELECT 'bitpacking tail isNotNull indexed';
SELECT id FROM text_index_defined_bitpacking WHERE isNotNull(value) AND id >= 8 ORDER BY id;
SELECT 'bitpacking tail isNotNull scan';
SELECT id FROM text_index_defined_bitpacking WHERE isNotNull(value) AND id >= 8 ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

CREATE TABLE text_index_defined_fixed_string
(
    id UInt32,
    value Nullable(FixedString(8)),
    INDEX i value TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES text_index_defined_fixed_string;

INSERT INTO text_index_defined_fixed_string VALUES
    (0, 'alpha'), (1, NULL), (2, ''), (3, 'beta');

SELECT 'fixed isNull indexed';
SELECT id FROM text_index_defined_fixed_string WHERE isNull(value) ORDER BY id;
SELECT 'fixed isNull scan';
SELECT id FROM text_index_defined_fixed_string WHERE isNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'fixed isNotNull indexed';
SELECT id FROM text_index_defined_fixed_string WHERE isNotNull(value) ORDER BY id;
SELECT 'fixed isNotNull scan';
SELECT id FROM text_index_defined_fixed_string WHERE isNotNull(value) ORDER BY id
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE text_index_defined_bitpacking;
DROP TABLE text_index_defined_fixed_string;
