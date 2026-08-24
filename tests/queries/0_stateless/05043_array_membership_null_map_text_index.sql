-- Tags: no-parallel-replicas
-- A text index answers hasAll and hasAny with its own evaluator, so it must agree with the
-- ordinary array path on an array whose NULL element hides a value equal to the needle.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_null_map_text;

CREATE TABLE t_null_map_text
(
    id UInt32,
    arr Array(Nullable(String)),
    INDEX idx arr TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

-- id 1 stores 'zz' underneath a NULL, so it does not contain 'zz'. id 2 really contains it.
INSERT INTO t_null_map_text SELECT 1, arrayMap(x -> nullIf(x, 'zz'), ['a', 'zz', 'c']);
INSERT INTO t_null_map_text SELECT 2, ['a', 'zz', 'c'];

SELECT '-- has(), which is the expected answer for every arm below --';
SELECT id, has(arr, 'zz') FROM t_null_map_text ORDER BY id;

SELECT '-- hasAll and hasAny without the index --';
SELECT id, hasAll(arr, ['zz']), hasAny(arr, ['zz'])
FROM t_null_map_text ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- matching ids, index off --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- matching ids, index on, reading through the index --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT '-- matching ids, index on, pruning only --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT id FROM t_null_map_text WHERE hasAny(arr, ['zz']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;

SELECT '-- control: a needle that is genuinely present in both rows --';
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['a']) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_null_map_text WHERE hasAll(arr, ['a']) ORDER BY id
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE t_null_map_text;
