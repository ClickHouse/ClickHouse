-- A dictionary definition in the AST JSON format is rejected when a key-value pair has no value.

SET enable_json_ast_dialect = 1;

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$);

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"first":"table","second_with_brackets":false,"second":', '"first":"table","second_with_brackets":false,"unused_second":')); -- { serverError BAD_ARGUMENTS }
