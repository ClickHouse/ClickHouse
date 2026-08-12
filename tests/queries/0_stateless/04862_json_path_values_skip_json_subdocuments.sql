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

SELECT groupArray(id)
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

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.b = 2
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.m['k'] = 'v'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.nested.x = 'leaf'
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.u = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.@u = 42;

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.@u = 42
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.t = tuple(10, 'inside')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc.t.n = 10
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.blocked.m['k'] = 'v';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE mapContains(data.blocked.m, 'k');

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.blocked.m['k'] = 'v'
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE mapContains(data.blocked.m, 'k')
SETTINGS optimize_functions_to_subcolumns = 1, force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.t = tuple(1, 'a')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT groupArray(id)
FROM json_path_values_skip_json_subdocuments
WHERE data.doc = '{"":"empty","b":2,"m":{"k":"v"},"nested":{"x":"leaf"},"t":{"n":10,"s":"inside"},"u":42}'::JSON(
    max_dynamic_paths = 0,
    b Int64,
    m Map(String, String),
    nested JSON(max_dynamic_paths = 0, x String),
    t Tuple(n Int64, s String))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_skip_json_subdocuments;
