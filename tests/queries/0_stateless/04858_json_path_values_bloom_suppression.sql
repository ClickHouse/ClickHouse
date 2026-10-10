SET enable_json_type = 1;
SET optimize_functions_to_subcolumns = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;

DROP TABLE IF EXISTS json_path_values_bloom_suppression;
CREATE TABLE json_path_values_bloom_suppression
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, exact Nullable(String), presence Nullable(String)),
    INDEX values_idx data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1,
    INDEX paths_idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_bloom_suppression VALUES
    (1, '{"exact":"hit","presence":"present"}'),
    (2, '{"exact":"miss","presence":null}'),
    (3, '{}');

SELECT 'exact';
SELECT substring(explain, position(explain, 'Name:'))
FROM (EXPLAIN indexes = 1 SELECT * FROM json_path_values_bloom_suppression WHERE data.exact = 'hit')
WHERE explain LIKE '%Name:%';

SELECT 'in';
SELECT substring(explain, position(explain, 'Name:'))
FROM (EXPLAIN indexes = 1 SELECT * FROM json_path_values_bloom_suppression WHERE data.exact IN ('hit'))
WHERE explain LIKE '%Name:%';

SELECT 'is not null';
SELECT countIf(explain LIKE '%Name: values_idx%') = 0
FROM (EXPLAIN indexes = 1 SELECT * FROM json_path_values_bloom_suppression WHERE isNotNull(data.presence));
SELECT arraySort(groupArray(id)) FROM json_path_values_bloom_suppression WHERE isNotNull(data.presence);

SELECT 'mixed';
SELECT substring(explain, position(explain, 'Name:'))
FROM
(
    EXPLAIN indexes = 1
    SELECT *
    FROM json_path_values_bloom_suppression
    WHERE data.exact = 'hit' AND isNotNull(data.presence)
)
WHERE explain LIKE '%Name:%';

DROP TABLE IF EXISTS json_path_values_bloom_partial;
CREATE TABLE json_path_values_bloom_partial
(
    id UInt64,
    data JSON(max_dynamic_paths = 0, exact Nullable(String)),
    INDEX paths_idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_bloom_partial VALUES (1, '{"exact":"hit"}');
ALTER TABLE json_path_values_bloom_partial
    ADD INDEX values_idx data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1;

SELECT 'not materialized';
SELECT substring(explain, position(explain, 'Name:'))
FROM (EXPLAIN indexes = 1 SELECT * FROM json_path_values_bloom_partial WHERE data.exact = 'hit')
WHERE explain LIKE '%Name:%';

DROP TABLE json_path_values_bloom_partial;
DROP TABLE json_path_values_bloom_suppression;
