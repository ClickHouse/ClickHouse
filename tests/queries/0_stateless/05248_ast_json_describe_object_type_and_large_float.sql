-- Statements that parseQueryToJSON could not serialize (DESCRIBE, JSON type arguments in column definitions) and a
-- Float64 literal that formatQueryFromJSON could not read back from its own output (written as an integer beyond
-- 64 bits). Found while measuring what the JSON AST does not cover with json_ast_sql_parser_fuzzer's seed corpus.
SELECT formatQueryFromJSON(parseQueryToJSON('DESCRIBE TABLE t'));
SELECT formatQueryFromJSON(parseQueryToJSON('DESC TEMPORARY TABLE t FORMAT TSV'));
SELECT formatQueryFromJSON(parseQueryToJSON('DESCRIBE (SELECT 1 AS x, ''a'' AS y)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (c JSON(a Map(String, Int64), `a.keys` Int64, SKIP b, SKIP REGEXP ''^x'', max_dynamic_paths = 10)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT CAST(''{}'', ''JSON(max_dynamic_types = 2, SKIP a.b)'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 9.99e19, -1e19, 1e300, 18446744073709551616, 1.5, 100.0, 1e-7'));
SELECT parseQueryToJSON('SELECT 9.99e19') LIKE '%"value":99900000000000000000.0%';
-- An ObjectTypeArgument needs exactly one of its alternatives.
SELECT formatQueryFromJSON('{"type":"ObjectTypeArgument"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"DescribeQuery"}'); -- { serverError BAD_ARGUMENTS }
