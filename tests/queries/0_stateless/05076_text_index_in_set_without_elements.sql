-- The text index on `mapKeys` is usable for `m['key'] IN (...)`: the condition can only hold when the
-- key is present, which the index can answer. Deciding that requires the `IN` set to *exist* (the
-- sub-DAG is evaluated on a default map, and `FunctionIn` needs a ready set) but never reads its
-- elements. A set larger than `use_index_for_in_with_subqueries_max_values` is built without storing
-- its elements, and requiring them dropped the index for exactly the large sets it is most useful on.
-- The same holds for a JSON path, which is a separate implementation of the same decision, so both
-- are pinned here: covering only one leaves the other free to regress.

DROP TABLE IF EXISTS t_text_in_set;

CREATE TABLE t_text_in_set
(
    id UInt64,
    m Map(String, String),
    INDEX idx_tk mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

INSERT INTO t_text_in_set VALUES (0, {'hello world':'val0'}), (1, {'foo bar':'val1'}), (2, {'baz qux':'val2'});

SELECT '-- set elements are stored: index is used';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_text_in_set
    WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
    SETTINGS use_index_for_in_with_subqueries_max_values = 0
) WHERE explain LIKE '%Granules:%';

SELECT '-- set is above the limit, so its elements are dropped: index is still used';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_text_in_set
    WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
    SETTINGS use_index_for_in_with_subqueries_max_values = 2
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer does not depend on the limit';
SELECT id FROM t_text_in_set
WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
ORDER BY id
SETTINGS use_index_for_in_with_subqueries_max_values = 0;

SELECT id FROM t_text_in_set
WHERE m['hello world'] IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
ORDER BY id
SETTINGS use_index_for_in_with_subqueries_max_values = 2;

SELECT '-- the JSON path is a separate implementation of the same decision';

DROP TABLE IF EXISTS t_json_text_in_set;

CREATE TABLE t_json_text_in_set
(
    id UInt64,
    json JSON,
    INDEX idx_jp JSONAllPaths(json) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;

INSERT INTO t_json_text_in_set VALUES (0, '{"a": "val0"}'), (1, '{"b": "val1"}'), (2, '{"c": "val2"}');

SELECT '-- set elements are stored: index is used';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_json_text_in_set
    WHERE json.a.:String IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
    SETTINGS use_index_for_in_with_subqueries_max_values = 0
) WHERE explain LIKE '%Granules:%';

SELECT '-- set is above the limit, so its elements are dropped: index is still used';
SELECT trim(replaceRegexpOne(explain, '^[^A-Za-z]+', '')) FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_json_text_in_set
    WHERE json.a.:String IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
    SETTINGS use_index_for_in_with_subqueries_max_values = 2
) WHERE explain LIKE '%Granules:%';

SELECT '-- and the answer does not depend on the limit';
SELECT id FROM t_json_text_in_set
WHERE json.a.:String IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
ORDER BY id
SETTINGS use_index_for_in_with_subqueries_max_values = 0;

SELECT id FROM t_json_text_in_set
WHERE json.a.:String IN (SELECT arrayJoin(['val0', 'val1', 'val2']))
ORDER BY id
SETTINGS use_index_for_in_with_subqueries_max_values = 2;

DROP TABLE t_text_in_set;
DROP TABLE t_json_text_in_set;
