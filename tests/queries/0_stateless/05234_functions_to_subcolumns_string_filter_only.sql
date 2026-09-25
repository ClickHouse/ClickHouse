SET enable_analyzer = 1;
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS test_string_filter_only;
CREATE TABLE test_string_filter_only
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS string_serialization_version = 'with_size_stream';

INSERT INTO test_string_filter_only VALUES (0, ''), (1, 'hello'), (2, ''), (3, 'world');

SELECT 'length uses String size with full column';
SELECT countIf(explain ILIKE '%s.size%') > 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE length(s) > 0
    SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0);

SELECT 'empty uses String size with full column';
SELECT countIf(explain ILIKE '%s.size%') > 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE empty(s)
    SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0);

SELECT 'notEmpty uses String size with full column';
SELECT countIf(explain ILIKE '%s.size%') > 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE notEmpty(s)
    SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0);

SELECT 'full String stays available after the filter';
SELECT id, s
FROM test_string_filter_only
PREWHERE notEmpty(s)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;

SELECT 'optimization disabled keeps the original filter';
SELECT countIf(explain ILIKE '%s.size%') = 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE notEmpty(s)
    SETTINGS optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0);

SELECT id, s
FROM test_string_filter_only
PREWHERE notEmpty(s)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0;

-- Keep the default comparison rewrite enabled: comparisons must compose with
-- the size-subcolumn rewrite even though the query still needs the full String.
SELECT 'empty-string equality uses String size with full column';
SELECT countIf(explain ILIKE '%s.size%') > 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE s = ''
    SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0);
SELECT s, id
FROM test_string_filter_only
PREWHERE s = ''
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;

SELECT 'empty-string inequality uses String size with full column';
SELECT countIf(explain ILIKE '%s.size%') > 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE s != ''
    SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0);
SELECT s, id
FROM test_string_filter_only
PREWHERE s != ''
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;

SELECT 'reversed comparison in WHERE keeps the full String';
SELECT s, id
FROM test_string_filter_only
WHERE '' <> s
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 1;

SELECT 'comparisons without the subcolumn rewrite keep the same results';
SELECT countIf(explain ILIKE '%s.size%') = 0
FROM (EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s
    FROM test_string_filter_only
    PREWHERE s != ''
    SETTINGS optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0);
SELECT s, id
FROM test_string_filter_only
PREWHERE s = ''
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0;
SELECT s, id
FROM test_string_filter_only
PREWHERE s != ''
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0;

DROP TABLE test_string_filter_only;

DROP TABLE IF EXISTS test_string_filter_mixed;
CREATE TABLE test_string_filter_mixed
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
    ratio_of_defaults_for_sparse_serialization = 0.8;

INSERT INTO test_string_filter_mixed
SELECT number, 0, if(number = 9, 'legacy', '')
FROM numbers(10);

ALTER TABLE test_string_filter_mixed MODIFY SETTING
    string_serialization_version = 'with_size_stream';

INSERT INTO test_string_filter_mixed VALUES (10, 1, ''), (11, 1, 'modern');

SELECT 'mixed table has sparse legacy and size-stream parts';
SELECT
    countIf(serialization_kind = 'Sparse'),
    countIf(not has(substreams, 's.size')),
    countIf(has(substreams, 's.size'))
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table = 'test_string_filter_mixed'
    AND active
    AND column = 's';

SELECT 'mixed sparse legacy and size-stream parts remain correct';
SELECT id, s
FROM test_string_filter_mixed
PREWHERE notEmpty(s)
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;

SELECT 'empty-string comparisons on mixed legacy and size-stream parts';
SELECT s, id
FROM test_string_filter_mixed
PREWHERE '' != s
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;
SELECT s, id
FROM test_string_filter_mixed
PREWHERE s = ''
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1, optimize_move_to_prewhere = 0;

DROP TABLE test_string_filter_mixed;

DROP TABLE IF EXISTS test_string_filter_multistep;
CREATE TABLE test_string_filter_multistep
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    serialization_info_version = 'with_types',
    string_serialization_version = 'single_stream',
    ratio_of_defaults_for_sparse_serialization = 0;

INSERT INTO test_string_filter_multistep VALUES (0, ''), (1, 'foo'), (2, 'bar'), (3, 'foo'), (4, '');

SELECT 'legacy String size predicate written first in multi-step PREWHERE';
SELECT id, s
FROM test_string_filter_multistep
PREWHERE notEmpty(s) AND id > 0 AND like(s, '%foo%')
ORDER BY id
SETTINGS
    enable_multiple_prewhere_read_steps = 1,
    optimize_functions_to_subcolumns = 1,
    optimize_move_to_prewhere = 0;

SELECT 'legacy String full-String predicate written first in multi-step PREWHERE';
SELECT id, s
FROM test_string_filter_multistep
PREWHERE like(s, '%foo%') AND id > 0 AND notEmpty(s)
ORDER BY id
SETTINGS
    enable_multiple_prewhere_read_steps = 1,
    optimize_functions_to_subcolumns = 1,
    optimize_move_to_prewhere = 0;

DROP TABLE test_string_filter_multistep;
