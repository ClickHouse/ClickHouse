SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_review_predicates;
CREATE TABLE json_path_values_review_predicates
(
    id UInt64,
    data JSON(k Nullable(String), fixed Nullable(FixedString(3))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_predicates VALUES
    (1, '{"k":"v3"}'),
    (2, '{"k":"x","fixed":"abc"}'),
    (3, '{}');

SELECT 'FixedString set fallback';
SELECT arraySort(groupArray(id))
FROM json_path_values_review_predicates
WHERE ifNull(data.fixed, 'a') IN (SELECT toFixedString('a', 3));
SELECT count()
FROM json_path_values_review_predicates
WHERE ifNull(data.fixed, 'a') IN (SELECT toFixedString('a', 3))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'projected nullable predicate';
SELECT id, data.k = 'v3'
FROM json_path_values_review_predicates
WHERE data.k = 'v3' OR id >= 0
ORDER BY id;
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT id, data.k = 'v3'
    FROM json_path_values_review_predicates
    WHERE data.k = 'v3' OR id >= 0
)
WHERE position(explain, '__text_index') > 0;
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT id
    FROM json_path_values_review_predicates
    WHERE data.k = 'v3'
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_path_values_review_predicates;

DROP TABLE IF EXISTS json_path_values_review_map;
CREATE TABLE json_path_values_review_map
(
    id UInt64,
    data JSON(attrs Map(String, String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64), dictionary_block_size = 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_map
SELECT number, format('{{"attrs":{{"k":"v{0}"}}}}', number)
FROM numbers(8);

SELECT 'map key pruning';
SELECT count() = 1
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM json_path_values_review_map
    WHERE mapContains(data.attrs, 'missing')
)
WHERE explain LIKE '%Granules: 0/8%';
SELECT count()
FROM json_path_values_review_map
WHERE mapContains(data.attrs, 'missing')
SETTINGS force_data_skipping_indices = 'tokens';

DROP TABLE json_path_values_review_map;
