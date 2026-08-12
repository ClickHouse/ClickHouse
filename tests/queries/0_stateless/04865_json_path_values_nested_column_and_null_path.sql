SET enable_json_type = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;

DROP TABLE IF EXISTS json_path_values_nested_column;
CREATE TABLE json_path_values_nested_column
(
    id UInt64,
    t Tuple(j JSON(max_dynamic_paths = 0, s String)),
    INDEX values_idx t.j TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_nested_column VALUES (1, ('{"s":"hit"}',));
INSERT INTO json_path_values_nested_column VALUES (2, ('{"s":"miss"}',));
OPTIMIZE TABLE json_path_values_nested_column FINAL;

SELECT groupArray(id)
FROM json_path_values_nested_column
WHERE t.j.s = 'hit'
SETTINGS force_data_skipping_indices = 'values_idx';

DROP TABLE IF EXISTS json_path_values_literal_null;
CREATE TABLE json_path_values_literal_null
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, `literal.null` UInt8),
    INDEX paths_idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_literal_null VALUES
    (1, '{"literal":{"null":1}}'),
    (2, '{"literal":{"null":0}}'),
    (3, '{}');

SELECT arraySort(groupArray(id))
FROM json_path_values_literal_null
WHERE not(data.literal.null);

SELECT count()
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM json_path_values_literal_null WHERE not(data.literal.null)
)
WHERE explain LIKE '%Name: paths_idx%';

DROP TABLE json_path_values_literal_null;
DROP TABLE json_path_values_nested_column;
