-- A dictionary definition in the AST JSON format is rejected when a key-value pair has no value: in the source or
-- in the layout parameters, at any depth of a bracketed list, and whether `second` is absent or null.

SET enable_json_ast_dialect = 1;

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$);

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"first":"table","second_with_brackets":false,"second":', '"first":"table","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"second":{"type":"Literal","value":{"field_type":"String","value":"t"}}', '"second":null')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(0)$$);

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(0)$$),
    '"first":"headers","second_with_brackets":true,"second":', '"first":"headers","second_with_brackets":true,"unused_second":')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(0)$$),
    '"first":"name","second_with_brackets":false,"second":', '"first":"name","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(0)$$),
    '"first":"max_update_queue_size","second_with_brackets":false,"second":', '"first":"max_update_queue_size","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (a UInt64, b String, v String) PRIMARY KEY a, b SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 2)) LIFETIME(0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (a UInt64, b String, v String) PRIMARY KEY a, b SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 2)) LIFETIME(0)$$);

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (a UInt64, b String, v String) PRIMARY KEY a, b SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(COMPLEX_KEY_HASHED(SHARDS 2)) LIFETIME(0)$$),
    '"first":"shards","second_with_brackets":false,"second":', '"first":"shards","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, start Date, end Date, v String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')) RANGE(MIN start MAX end) LIFETIME(0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, start Date, end Date, v String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')) RANGE(MIN start MAX end) LIFETIME(0)$$);

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, start Date, end Date, v String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max')) RANGE(MIN start MAX end) LIFETIME(0)$$),
    '"first":"range_lookup_strategy","second_with_brackets":false,"second":', '"first":"range_lookup_strategy","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }
