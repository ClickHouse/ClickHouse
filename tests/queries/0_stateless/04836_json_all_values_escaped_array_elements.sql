DROP TABLE IF EXISTS json_all_values_escaped_array_elements;

CREATE TABLE json_all_values_escaped_array_elements
(
    id UInt8,
    data JSON(tags Array(String)),
    INDEX ngram_idx JSONAllValues(data) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1,
    INDEX sparse_idx JSONAllValues(data) TYPE sparse_grams(3, 100, 256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO json_all_values_escaped_array_elements VALUES
    (1, '{"tags":["abc''def","ghi\\\\jkl"]}');

SELECT id FROM json_all_values_escaped_array_elements
WHERE has(data.tags, 'abc''def')
SETTINGS use_skip_indexes = 0;

SELECT id FROM json_all_values_escaped_array_elements
WHERE has(data.tags, 'abc''def')
SETTINGS force_data_skipping_indices = 'ngram_idx';

SELECT id FROM json_all_values_escaped_array_elements
WHERE hasAny(data.tags, ['abc''def', 'missing'])
SETTINGS force_data_skipping_indices = 'ngram_idx';

SELECT id FROM json_all_values_escaped_array_elements
WHERE hasAll(data.tags, ['abc''def', 'ghi\\jkl'])
SETTINGS force_data_skipping_indices = 'ngram_idx';

SELECT id FROM json_all_values_escaped_array_elements
WHERE has(data.tags, 'abc''def')
SETTINGS force_data_skipping_indices = 'sparse_idx';

SELECT id FROM json_all_values_escaped_array_elements
WHERE hasAny(data.tags, ['abc''def', 'missing'])
SETTINGS force_data_skipping_indices = 'sparse_idx';

SELECT id FROM json_all_values_escaped_array_elements
WHERE hasAll(data.tags, ['abc''def', 'ghi\\jkl'])
SETTINGS force_data_skipping_indices = 'sparse_idx';

DROP TABLE json_all_values_escaped_array_elements;
