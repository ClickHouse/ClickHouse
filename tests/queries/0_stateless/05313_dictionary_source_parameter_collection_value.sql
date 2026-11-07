-- A dictionary source parameter does not accept a tuple or a map, including one that a constant
-- expression in the definition evaluates to: such a value has no stored form that loads back.

-- A Replicated database reports SYNTAX_ERROR: the evaluated tuple is replicated as text that does not parse.
CREATE DICTIONARY d_tuple (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL tuple('http://example.test/a', 'http://example.test/b') FORMAT 'TSV'))
LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS, SYNTAX_ERROR }

CREATE DICTIONARY d_tuple_single (id UInt64, v String) PRIMARY KEY id
SOURCE(CLICKHOUSE(DB tuple('db') TABLE 't'))
LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY d_map (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL 'http://example.test/' FORMAT 'TSV' HEADERS map('Authorization', 'token')))
LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }

-- A scalar constant expression is still evaluated, and the stored definition reads back.
CREATE DICTIONARY d_scalar (id UInt64) PRIMARY KEY id
SOURCE(HTTP(URL concat('http://example.test/', 'x') FORMAT 'TSV'))
LAYOUT(FLAT()) LIFETIME(0);
SHOW CREATE DICTIONARY d_scalar;
DROP DICTIONARY d_scalar;
