-- A text index can answer `m['key'] IN (...)` only because the predicate is false whenever the key
-- is missing, which is decided once, at analysis time, by evaluating the predicate on a default
-- value. A set backed by `ENGINE = Set` is not a snapshot: `StorageSet::insertBlock` inserts into
-- the very `Set` the query holds, so an `INSERT` of the default value landing after that decision
-- would make the predicate true for a missing key, while the granules holding such rows have
-- already been pruned - the query would silently miss them. So the index must not be used here,
-- however tempting it is that the decision itself never reads the set's elements.

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

SELECT '-- map key: no granule is pruned for a mutable `ENGINE = Set`';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_index WHERE m['hello world'] IN values_set
) WHERE explain LIKE '%Granules:%';

SELECT '-- an `IN` set of the same shape that the query owns is still pruned';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_map_text_index WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer is the same either way';
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

SELECT '-- JSON path: no granule is pruned for a mutable `ENGINE = Set`';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set
) WHERE explain LIKE '%Granules:%';

SELECT '-- an `IN` set of the same shape that the query owns is still pruned';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_json_text_index WHERE json.a.:String IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer is the same either way';
SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set ORDER BY id;
SELECT id FROM t_json_text_index WHERE json.a.:String IN values_set ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE t_map_text_index;
DROP TABLE t_json_text_index;
DROP TABLE values_set;
