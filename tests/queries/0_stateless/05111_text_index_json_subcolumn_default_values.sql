SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS test_json_subcolumn_default_values;

CREATE TABLE test_json_subcolumn_default_values
(
    id UInt32,
    data JSON(max_dynamic_paths = 16),
    INDEX idx_json JSONAllValues(data) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- The first and last rows have no indexed value for the queried paths, but their
-- non-nullable scalar casts return the same defaults as the second row.
INSERT INTO test_json_subcolumn_default_values VALUES
    (1, '{"other":"present"}'),
    (2, '{"i":0,"flag":false,"d":"1970-01-01"}'),
    (3, '{"i":1,"flag":true,"d":"2026-09-07"}'),
    (4, '{"i":null,"flag":null,"d":null}');

SELECT 'default values without the index';
SET use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 0;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('1970-01-01');

SELECT 'default values with the index, direct read disabled';
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 0;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 0;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('1970-01-01');

SELECT 'default comparisons do not select the index';
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 0
);
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.flag::Bool = false
);
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('1970-01-01')
);

SELECT 'default values with the index, direct read enabled';
SET query_plan_direct_read_from_text_index = 1;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 0;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.flag::Bool = false;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('1970-01-01');

SELECT 'constant on the left and a converted string constant';
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE 0 = CAST(data.i, 'UInt64');
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = '0';

SELECT 'non-default values remain indexed';
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 1;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.flag::Bool = true;
SELECT arraySort(groupArray(id)) FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('2026-09-07');
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.i::UInt64 = 1
);
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.flag::Bool = true
);
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE data.d::Date = toDate('2026-09-07')
);

-- A `Nullable` cast preserves missing values as `NULL`, so equality to zero
-- cannot match those rows and remains safe to accelerate.
SELECT 'nullable casts remain indexed';
SELECT arraySort(groupArray(id))
FROM test_json_subcolumn_default_values
WHERE CAST(data.i, 'Nullable(UInt64)') = 0
SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id))
FROM test_json_subcolumn_default_values
WHERE CAST(data.i, 'Nullable(UInt64)') = 0;
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE CAST(data.i, 'Nullable(UInt64)') = 0
);

SELECT 'nested boolean defaults';
SELECT arraySort(groupArray(id))
FROM test_json_subcolumn_default_values
WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false)
SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id))
FROM test_json_subcolumn_default_values
WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false);
SELECT countIf(position(explain, 'Name: idx_json') > 0)
FROM
(
    EXPLAIN indexes = 1
    SELECT id FROM test_json_subcolumn_default_values WHERE CAST(data.missing, 'Tuple(Bool)') = tuple(false)
);

DROP TABLE test_json_subcolumn_default_values;
