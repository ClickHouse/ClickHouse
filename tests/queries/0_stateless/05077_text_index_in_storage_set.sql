-- A set backed by `ENGINE = Set` never stores its elements in explicit form, so
-- `FutureSetFromStorage::buildOrderedSetInplace` returns nothing for it. Deciding whether a text
-- index can answer `m['key'] IN set_table` (and the JSON path equivalent) only needs the set to
-- exist - the subDAG is evaluated on a default value and `FunctionIn` needs a ready set, but its
-- elements are never read - so requiring them dropped the index for every `StorageSet`.

DROP TABLE IF EXISTS t_map_text_index;
DROP TABLE IF EXISTS t_json_text_index;
DROP TABLE IF EXISTS values_set;

CREATE TABLE values_set (v String) ENGINE = Set;
INSERT INTO values_set VALUES ('val0'), ('val1'), ('val2');

CREATE TABLE t_map_text_index
(
    id UInt64,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

INSERT INTO t_map_text_index VALUES (0, {'hello world':'val0'}), (1, {'foo bar':'val1'}), (2, {'baz qux':'val2'});

SELECT '-- map key: the index is used for a set that stores no elements';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_index WHERE m['hello world'] IN values_set
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer is the same as without the index';
SELECT id FROM t_map_text_index WHERE m['hello world'] IN values_set ORDER BY id;
SELECT id FROM t_map_text_index WHERE m['hello world'] IN values_set ORDER BY id SETTINGS use_skip_indexes = 0;

CREATE TABLE t_json_text_index
(
    id UInt64,
    json JSON,
    INDEX idx_paths JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

INSERT INTO t_json_text_index VALUES (0, '{"a": "val0"}'), (1, '{"b": "val1"}'), (2, '{"c": "val2"}');

SELECT '-- JSON path: the index is used for a set that stores no elements';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer is the same as without the index';
SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set ORDER BY id;
SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE t_map_text_index;
DROP TABLE t_json_text_index;
DROP TABLE values_set;
