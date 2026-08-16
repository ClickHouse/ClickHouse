SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_path_values_review_round_2;
CREATE TABLE json_path_values_review_round_2
(
    id UInt64,
    data JSON(k String, s Nullable(String)),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_review_round_2 VALUES
    (1, '{"k":"a","s":"needle"}'),
    (2, '{"k":"b","s":"other"}'),
    (3, '{"k":"c","s":null}'),
    (4, '{"k":"d"}');

SELECT toTypeName(CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))'));
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))');

SELECT toTypeName(CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))'));
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE data.k = CAST(toNullable(toFixedString('a', 3)), 'LowCardinality(Nullable(FixedString(3)))');

SELECT count() = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE data.k = CAST(toFixedString('a', 3), 'LowCardinality(FixedString(3))')
)
WHERE explain LIKE '%Name: tokens%';

SELECT id, match(data.s, 'needle') AS matched
FROM json_path_values_review_round_2
WHERE id IN (3, 4) OR matched
ORDER BY id
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT id, match(data.s, 'needle') AS matched
FROM json_path_values_review_round_2
WHERE id IN (3, 4) OR matched
ORDER BY id;

SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE NOT match(data.s, 'needle')
SETTINGS use_skip_indexes = 0, use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id))
FROM json_path_values_review_round_2
WHERE NOT match(data.s, 'needle');

SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE NOT match(data.s, 'needle')
)
WHERE explain LIKE '%__text_index%';

SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count()
    FROM json_path_values_review_round_2
    WHERE match(data.s, 'needle')
)
WHERE explain LIKE '%__text_index%';

DROP TABLE json_path_values_review_round_2;
