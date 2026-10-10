SET enable_json_type = 1;

DROP TABLE IF EXISTS json_path_values_skip_json_subdocuments;

CREATE TABLE json_path_values_skip_json_subdocuments
(
    id UInt64,
    data JSON(
        max_dynamic_paths = 0,
        blocked Tuple(m Map(String, String)),
        doc JSON(
            max_dynamic_paths = 0,
            b Int64,
            m Map(String, String),
            nested JSON(max_dynamic_paths = 0, x String),
            t Tuple(n Int64, s String)),
        t Tuple(n Int64, s String),
        s String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_skip_json_subdocuments VALUES
    (1, '{"blocked":{"m":{"k":"v"}},"doc":{"":"empty","b":2,"m":{"k":"v"},"nested":{"x":"leaf"},"t":{"n":10,"s":"inside"},"u":42},"t":{"n":1,"s":"a"},"s":"x"}'),
    (2, '{"blocked":{"m":{"other":"value"}},"doc":{"b":3,"m":{"other":"value"},"nested":{"x":"other"},"t":{"n":20,"s":"elsewhere"},"u":43},"t":{"n":2,"s":"b"},"s":"y"}');

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc = '{"":"empty","b":2,"m":{"k":"v"},"nested":{"x":"leaf"},"t":{"n":10,"s":"inside"},"u":42}'::JSON(
    max_dynamic_paths = 0,
    b Int64,
    m Map(String, String),
    nested JSON(max_dynamic_paths = 0, x String),
    t Tuple(n Int64, s String));

SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_json_subdocuments', 'tokens')
WHERE startsWith(hex(token), '646F630000');

SELECT count()
FROM mergeTreeTextIndex(currentDatabase(), 'json_path_values_skip_json_subdocuments', 'tokens')
WHERE startsWith(hex(token), '646F632E0000');

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.b = 2
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.m['k'] = 'v'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.nested.x = 'leaf'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.u = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.@u = 42;

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.@u = 42
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.t = tuple(10, 'inside')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.t.n = 10
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.blocked.m['k'] = 'v';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE mapContains(data.blocked.m, 'k');

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.blocked.m['k'] = 'v'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE mapContains(data.blocked.m, 'k')
SETTINGS optimize_functions_to_subcolumns = 1, force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.t = tuple(1, 'a')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT arraySort(groupArray(id))
FROM json_path_values_skip_json_subdocuments
WHERE data.doc = '{"":"empty","b":2,"m":{"k":"v"},"nested":{"x":"leaf"},"t":{"n":10,"s":"inside"},"u":42}'::JSON(
    max_dynamic_paths = 0,
    b Int64,
    m Map(String, String),
    nested JSON(max_dynamic_paths = 0, x String),
    t Tuple(n Int64, s String))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_skip_json_subdocuments;
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

SELECT arraySort(groupArray(id))
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
SET use_skip_indexes_on_data_read = 1;
SET optimize_empty_string_comparisons = 1;

DROP TABLE IF EXISTS json_path_values_nested_subcolumns;
CREATE TABLE json_path_values_nested_subcolumns
(
    id UInt64,
    data JSON(
        str String,
        arr Array(String),
        n Nullable(Int64),
        tuple Tuple(value String),
        map Map(String, String)),
    INDEX data_tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_nested_subcolumns VALUES
    (1, '{"str":"a","arr":["x","y"],"n":1,"tuple":{"value":"x"},"map":{"k":"v"}}'),
    (2, '{"str":"","arr":[],"n":null,"tuple":{"value":"y"},"map":{}}'),
    (3, '{"str":"c","arr":["z"],"tuple":{"value":"x"},"map":{"q":"w"}}');

SELECT 'optimized subcolumns';
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE data.str = '';
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE empty(data.str);
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE length(data.arr) = 2;
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE empty(data.arr);

SELECT 'nested typed subcolumns';
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE data.tuple.value = 'x';
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE has(data.map.keys, 'k');
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE data.n.null = 1;

SELECT count() FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_nested_subcolumns WHERE length(data.arr) = 2
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.tuple.value = 'x'
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE has(data.map.keys, 'k')
SETTINGS force_data_skipping_indices = 'data_tokens';
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.n.null = 1
SETTINGS force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'direct typed path';
SELECT arraySort(groupArray(id)) FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS
    optimize_empty_string_comparisons = 0,
    optimize_functions_to_subcolumns = 0;
SELECT count() FROM json_path_values_nested_subcolumns WHERE data.str = ''
SETTINGS
    optimize_empty_string_comparisons = 0,
    optimize_functions_to_subcolumns = 0,
    force_data_skipping_indices = 'data_tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_nested_subcolumns;
