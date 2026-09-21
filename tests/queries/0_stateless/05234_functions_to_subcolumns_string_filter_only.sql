SET enable_analyzer = 1;

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

DROP TABLE test_string_filter_only;
