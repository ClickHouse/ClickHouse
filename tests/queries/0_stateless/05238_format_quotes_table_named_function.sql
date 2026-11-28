-- A table named `function` in INSERT INTO was formatted unquoted and read back as INSERT INTO FUNCTION.
-- Found by json_ast_sql_parser_fuzzer (JSON_AST_FUZZER_STRICT=reparse).
SELECT formatQuerySingleLine('INSERT INTO `function` SELECT 1');
SELECT formatQuerySingleLine(formatQuerySingleLine('INSERT INTO `function` SELECT 1'));
SELECT formatQuerySingleLine('SELECT `function`, `function` + 1 FROM `function`');
