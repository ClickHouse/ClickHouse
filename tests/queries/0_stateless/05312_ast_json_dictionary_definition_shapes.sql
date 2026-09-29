-- A dictionary definition in the AST JSON format must have a shape that the SQL parser produces.
-- The other shapes either stopped the server when the dictionary was created, or were stored as a
-- definition that does not parse back, so the server could not load it at the next start.

SET enable_json_ast_dialect = 1;

-- Positives: nested lists, single values, literal defaults, layout parameters, a function value,
-- an identifier value, an empty bracketed list, a two-column key, and the short ATTACH form.

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x', n Int64 DEFAULT -1, a Array(UInt8) DEFAULT [1, 2]) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'A' VALUE 'B') HEADER(NAME 'C' VALUE 'D')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(300)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x', n Int64 DEFAULT -1, a Array(UInt8) DEFAULT [1, 2]) PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'A' VALUE 'B') HEADER(NAME 'C' VALUE 'D')))) LAYOUT(CACHE(SIZE_IN_CELLS 10 MAX_UPDATE_QUEUE_SIZE 100)) LIFETIME(300)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (a UInt64, b String) PRIMARY KEY a, b SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE t REPLICA(HOST 'r' PRIORITY 1) REPLICA())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 0 MAX 10)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (a UInt64, b String) PRIMARY KEY a, b SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE t REPLICA(HOST 'r' PRIORITY 1) REPLICA())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 0 MAX 10)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$ATTACH DICTIONARY d$$)) = formatQuerySingleLine($$ATTACH DICTIONARY d$$);

-- Negatives: each query breaks one rule of the parser.

-- A list of pairs without brackets.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"first":"headers","second_with_brackets":true', '"first":"headers","second_with_brackets":false')); -- { serverError BAD_ARGUMENTS }

-- A single value in brackets, in a source and in a layout.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER 'v'))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"first":"header","second_with_brackets":false', '"first":"header","second_with_brackets":true')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(300)$$),
    '"first":"size_in_cells","second_with_brackets":false', '"first":"size_in_cells","second_with_brackets":true')); -- { serverError BAD_ARGUMENTS }

-- A list element that is not a pair.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '{"type":"Pair","first":"header"', '{"type":"Asterisk","first":"header"')); -- { serverError BAD_ARGUMENTS }

-- A source without brackets.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"name":"http","has_brackets":true', '"name":"http","has_brackets":false')); -- { serverError BAD_ARGUMENTS }

-- A primary key that is not an identifier, and an empty primary key.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"primary_key":{"type":"ExpressionList","children":[{"type":"Identifier"', '"primary_key":{"type":"ExpressionList","children":[{"type":"Asterisk"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"primary_key":{"type":"ExpressionList","children":', '"primary_key":{"type":"ExpressionList","unused_children":')); -- { serverError BAD_ARGUMENTS }

-- A default value that is not a literal.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"default_value":{"type":"Literal"', '"default_value":{"type":"Asterisk"')); -- { serverError BAD_ARGUMENTS }

-- A definition without attributes.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, v String DEFAULT 'x') PRIMARY KEY id SOURCE(HTTP(URL 'http://localhost/' FORMAT 'TabSeparated' HEADERS(HEADER(NAME 'k' VALUE 'v')))) LAYOUT(FLAT()) LIFETIME(0)$$),
    '"dictionary_attributes_list":', '"unused_dictionary_attributes_list":')); -- { serverError BAD_ARGUMENTS }
