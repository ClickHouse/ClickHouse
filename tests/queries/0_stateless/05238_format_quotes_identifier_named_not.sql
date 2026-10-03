-- An identifier named `not` was formatted without quotes, and `SELECT NOT not` does not parse back.
-- Found by json_ast_sql_parser_fuzzer (JSON_AST_FUZZER_STRICT=reparse).
SELECT formatQuerySingleLine('SELECT NOT `not`');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT NOT `not`'));
SELECT formatQuerySingleLine('SELECT `not`, -`not`, `not` + 1 FROM t WHERE `not`');
SELECT formatQuerySingleLine('SELECT not(1), `not`(1), NOT 1');

-- A table named `function` in INSERT INTO was formatted unquoted and read back as INSERT INTO FUNCTION.
SELECT formatQuerySingleLine('INSERT INTO `function` SELECT 1');
SELECT formatQuerySingleLine(formatQuerySingleLine('INSERT INTO `function` SELECT 1'));
