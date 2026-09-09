-- Tags: no-old-analyzer
-- no-old-analyzer: `eval` requires the analyzer; the rejection of the old analyzer is tested explicitly at the end of this test.

-- Table function `eval`: evaluates a constant expression to a query string and executes it.
-- See https://github.com/ClickHouse/ClickHouse/issues/101293

SELECT * FROM eval('SELECT 1'); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_eval_table_function = 1;

SELECT count() FROM system.table_functions WHERE name = 'eval';

-- Constant expressions.
SELECT * FROM eval('SEL' || 'ECT 1 AS x');
SELECT x + 1 FROM eval('SELECT 4 AS x');
SELECT * FROM eval(concat('SELECT ', toString(40 + 2), ' AS answer'));

-- Query parameters.
SET param_q = 'SELECT 2 AS y';
SELECT * FROM eval({q:String});

-- An input SELECT query is syntactic sugar for a scalar subquery and must return
-- a single row with a single String value.
SELECT * FROM eval(SELECT 'SELECT 3 AS z');
SELECT * FROM eval((SELECT 'SELECT 3 AS z'));
SELECT * FROM eval(SELECT toLowCardinality('SELECT 5 AS lc'));
SELECT * FROM eval(SELECT CAST('SELECT 6 AS n', 'Nullable(String)'));
SELECT * FROM eval(SELECT toLowCardinality(CAST('SELECT 7 AS lcn', 'Nullable(String)')));
SELECT * FROM eval(SELECT 'SELECT 8 AS u UNION ALL SELECT 9 AS u') ORDER BY u;

-- The argument is resolved in the outer scope as any other table function argument,
-- so it can use WITH-defined aliases of the outer query.
WITH 'SELECT 10 AS with_alias_value' AS q SELECT * FROM eval(q);
WITH 'SEL' AS a, 'ECT 11 AS with_concat_value' AS b SELECT * FROM eval(a || b);

-- The structure of the generated query is known at analysis time.
SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM eval('SELECT 12 AS explain_value'));

-- UNION/INTERSECT/EXCEPT in the generated query follow the usual set operation modes.
SELECT n FROM eval('SELECT 2 AS n UNION ALL SELECT 1 AS n') ORDER BY n;
SELECT * FROM eval('SELECT 1 AS n INTERSECT SELECT 1 AS n');
SELECT * FROM eval('SELECT 1 AS n EXCEPT SELECT 2 AS n');
SELECT count() FROM eval('SELECT 1 AS n UNION SELECT 1 AS n'); -- { serverError EXPECTED_ALL_OR_DISTINCT }
SET union_default_mode = 'DISTINCT';
SELECT count() FROM eval('SELECT 1 AS n UNION SELECT 1 AS n');
SET union_default_mode = DEFAULT;

-- SETTINGS of the generated query are scoped to the generated query.
SELECT count() FROM eval('SELECT number FROM numbers(3) SETTINGS limit = 1');

-- But the generated query cannot flip the analyzer setting, same as a usual query cannot
-- change it in a subquery.
SELECT * FROM eval('SELECT 31 SETTINGS enable_analyzer = 1');
SELECT * FROM eval('SELECT 31 SETTINGS enable_analyzer = 0'); -- { serverError INCORRECT_QUERY }
SELECT * FROM eval('SELECT 31 SETTINGS allow_experimental_analyzer = 0'); -- { serverError INCORRECT_QUERY }

-- The generated query must be self-contained: it cannot use WITH aliases of the outer query.
WITH 13 AS outer_alias SELECT * FROM eval('SELECT outer_alias'); -- { serverError UNKNOWN_IDENTIFIER }

-- The value must be a non-NULL string produced by a constant expression.
SELECT * FROM eval(14); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM eval(SELECT 15); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM eval(CAST(NULL, 'Nullable(String)')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(SELECT 'SELECT 16' WHERE 0); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(SELECT arrayJoin(['SELECT 17', 'SELECT 18'])); -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }
SELECT * FROM eval(toString(rand())); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval('SELECT 19', 'SELECT 20'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM eval(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The generated query must be a single SELECT query without output options.
SELECT * FROM eval('SHOW TABLES'); -- { serverError SYNTAX_ERROR }
SELECT * FROM eval('SELECT 21; SELECT 22'); -- { serverError SYNTAX_ERROR }
SELECT * FROM eval('SELECT 23 INTO OUTFILE ''eval.tsv'''); -- { serverError SYNTAX_ERROR }
SELECT * FROM eval('SELECT 24 FORMAT Null'); -- { serverError SYNTAX_ERROR }
SELECT * FROM eval('INSERT INTO t VALUES (1)'); -- { serverError SYNTAX_ERROR }

-- The generated query cannot use `eval` again.
SELECT * FROM eval('SELECT * FROM eval(''SELECT 25'')'); -- { serverError BAD_ARGUMENTS }

-- `eval` cannot be used as an argument of another table function.
SELECT * FROM remote('127.0.0.2', eval('SELECT 26')); -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM loop(eval('SELECT 27')) LIMIT 1; -- { serverError UNSUPPORTED_METHOD }

-- However, it can be used inside a query argument of `view`.
SELECT * FROM view(SELECT * FROM eval('SELECT 30 AS v'));

-- `eval` cannot be used to create a persisted table: the source expression would be re-evaluated
-- on every ATTACH and could depend on settings, parameters, or time.
CREATE TABLE t_eval AS eval('SELECT 1 AS x'); -- { serverError BAD_ARGUMENTS }

-- The generated query is opaque to the query result cache, so `eval` is treated as potentially
-- non-deterministic and potentially reading a system table, and is not cached by default.
SELECT * FROM eval('SELECT now()') SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT * FROM eval('SELECT * FROM system.one') SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'save'; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }

-- The old analyzer is not supported.
SET enable_analyzer = 0;
SELECT * FROM eval('SELECT 28'); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM eval(SELECT 'SELECT 29'); -- { serverError NOT_IMPLEMENTED }
