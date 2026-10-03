-- The parser accepts back-quoted names for the keys, key-value functions and the layout of `CREATE DICTIONARY`,
-- but the formatter wrote them upper-cased and unquoted (`SOURCE(CLICKHOUSE(MY KEY 1))`), which does not parse back.
-- Found by json_ast_sql_parser_fuzzer (JSON_AST_FUZZER_STRICT=reparse).
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(`my key` 1)) LAYOUT(FLAT()) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(`my src`(host 1)) LAYOUT(FLAT()) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(DB (`my src`(x 1)))) LAYOUT(FLAT()) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(host 1)) LAYOUT(`my layout`()) LIFETIME(0)');
-- Formatting is idempotent on the result.
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(`my src`(`my key` (`my nested`(x 1)))) LAYOUT(`my layout`()) LIFETIME(0)'));
-- Plain names keep the upper-cased form.
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(clickhouse(host ''localhost'' port 9000)) LAYOUT(flat()) LIFETIME(MIN 1 MAX 2)');

-- A registered source named like a keyword keeps its spelling (`isValidIdentifier` would back-quote `null`).
SELECT formatQuerySingleLine('CREATE DICTIONARY d (k UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)');
