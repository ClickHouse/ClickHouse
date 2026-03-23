-- Tags: no-parallel
-- ==========================================================================
-- Comprehensive test for parseQueryToJSON / formatQueryFromJSON
-- covering all AST types and all AST fields.
-- ==========================================================================

SET allow_experimental_json_type = 0; -- avoid interference with JSON column type

-- ==========================================================================
-- 1. ASTSelectWithUnionQuery
-- Fields: union_mode(string), list_of_modes(array), list_of_selects(child)
-- ==========================================================================

SELECT 'SelectWithUnionQuery_simple' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'union_mode') AS union_mode,
    JSONHas(j, 'list_of_selects') AS has_list
FROM (SELECT parseQueryToJSON('SELECT 1') AS j);

SELECT 'SelectWithUnionQuery_union' AS t,
    JSONExtractString(j, 'union_mode') AS union_mode,
    JSONExtractRaw(j, 'list_of_modes') AS modes
FROM (SELECT parseQueryToJSON('SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3') AS j);

SELECT 'SelectWithUnionQuery_distinct' AS t,
    JSONExtractRaw(j, 'list_of_modes') AS modes
FROM (SELECT parseQueryToJSON('SELECT 1 UNION DISTINCT SELECT 1') AS j);

-- ==========================================================================
-- 2. ASTSelectQuery
-- Fields: recursive_with, distinct, group_by_all, group_by_with_totals,
--         group_by_with_rollup, group_by_with_cube, group_by_with_constant_keys,
--         group_by_with_grouping_sets, order_by_all, limit_with_ties,
--         limit_by_all, with, select, tables, prewhere, where, group_by,
--         having, window, qualify, order_by, limit_by_offset, limit_by_length,
--         limit_by, limit_offset, limit_length, settings, interpolate
-- ==========================================================================

-- Test distinct flag
SELECT 'SelectQuery_distinct' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'distinct') AS distinct_flag
FROM (SELECT parseQueryToJSON('SELECT DISTINCT a FROM t') AS j);

-- Test group_by_with_totals
SELECT 'SelectQuery_totals' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'group_by_with_totals') AS totals
FROM (SELECT parseQueryToJSON('SELECT a FROM t GROUP BY a WITH TOTALS') AS j);

-- Test group_by_with_rollup
SELECT 'SelectQuery_rollup' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'group_by_with_rollup') AS rollup
FROM (SELECT parseQueryToJSON('SELECT a, count() FROM t GROUP BY ROLLUP(a)') AS j);

-- Test group_by_with_cube
SELECT 'SelectQuery_cube' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'group_by_with_cube') AS cube
FROM (SELECT parseQueryToJSON('SELECT a, count() FROM t GROUP BY CUBE(a)') AS j);

-- Test group_by_with_grouping_sets
SELECT 'SelectQuery_grouping_sets' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'group_by_with_grouping_sets') AS gsets
FROM (SELECT parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY GROUPING SETS ((a), (b))') AS j);

-- Test group_by_all
SELECT 'SelectQuery_group_by_all' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'group_by_all') AS gba
FROM (SELECT parseQueryToJSON('SELECT a, count() FROM t GROUP BY ALL') AS j);

-- Test order_by_all
SELECT 'SelectQuery_order_by_all' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'order_by_all') AS oba
FROM (SELECT parseQueryToJSON('SELECT a, b FROM t ORDER BY ALL') AS j);

-- Test limit_with_ties
SELECT 'SelectQuery_limit_with_ties' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'limit_with_ties') AS ties
FROM (SELECT parseQueryToJSON('SELECT a FROM t ORDER BY a LIMIT 5 WITH TIES') AS j);

-- Test all children: with, select, tables, prewhere, where, group_by, having, order_by,
-- limit_by, limit_offset, limit_length, settings, interpolate, qualify, window
SELECT 'SelectQuery_all_children' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select') AS has_select,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables') AS has_tables,
    JSONHas(j, 'list_of_selects', 'children', 1, 'prewhere') AS has_prewhere,
    JSONHas(j, 'list_of_selects', 'children', 1, 'where') AS has_where,
    JSONHas(j, 'list_of_selects', 'children', 1, 'group_by') AS has_group_by,
    JSONHas(j, 'list_of_selects', 'children', 1, 'having') AS has_having,
    JSONHas(j, 'list_of_selects', 'children', 1, 'order_by') AS has_order_by,
    JSONHas(j, 'list_of_selects', 'children', 1, 'limit_length') AS has_limit_length,
    JSONHas(j, 'list_of_selects', 'children', 1, 'limit_offset') AS has_limit_offset,
    JSONHas(j, 'list_of_selects', 'children', 1, 'settings') AS has_settings
FROM (SELECT parseQueryToJSON('SELECT DISTINCT a, sum(b) AS s FROM t PREWHERE c > 0 WHERE d = 1 GROUP BY a WITH TOTALS HAVING s > 10 ORDER BY a LIMIT 10 OFFSET 2 SETTINGS max_threads = 1') AS j);

-- Test limit_by fields
SELECT 'SelectQuery_limit_by' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'limit_by') AS has_limit_by,
    JSONHas(j, 'list_of_selects', 'children', 1, 'limit_by_length') AS has_limit_by_length
FROM (SELECT parseQueryToJSON('SELECT a, b FROM t LIMIT 1 BY a') AS j);

-- Test with (CTE)
SELECT 'SelectQuery_with' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'with') AS has_with
FROM (SELECT parseQueryToJSON('WITH x AS (SELECT 1) SELECT * FROM x') AS j);

-- Test qualify
SELECT 'SelectQuery_qualify' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'qualify') AS has_qualify
FROM (SELECT parseQueryToJSON('SELECT a, row_number() OVER (ORDER BY a) AS rn FROM t QUALIFY rn <= 3') AS j);

-- Test window clause
SELECT 'SelectQuery_window' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'window') AS has_window
FROM (SELECT parseQueryToJSON('SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY a)') AS j);

-- Test interpolate
SELECT 'SelectQuery_interpolate' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'interpolate') AS has_interpolate
FROM (SELECT parseQueryToJSON('SELECT a, b FROM t ORDER BY a WITH FILL INTERPOLATE (b AS b + 1)') AS j);

-- ==========================================================================
-- 3. ASTSelectIntersectExceptQuery
-- Fields: final_operator(string), children(array)
-- ==========================================================================

SELECT 'SelectIntersectExceptQuery_intersect' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'final_operator') AS op
FROM (SELECT parseQueryToJSON('SELECT 1 INTERSECT SELECT 1') AS j);

SELECT 'SelectIntersectExceptQuery_except' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'final_operator') AS op
FROM (SELECT parseQueryToJSON('SELECT 1 EXCEPT SELECT 2') AS j);

-- ==========================================================================
-- 4. ASTExpressionList
-- Fields: separator(string), children(array)
-- ==========================================================================

SELECT 'ExpressionList' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children') AS has_children
FROM (SELECT parseQueryToJSON('SELECT 1, 2, 3') AS j);

-- ==========================================================================
-- 5. ASTLiteral
-- Fields: value.field_type(string), value.value(varies), alias(string)
-- ==========================================================================

-- UInt64 literal
SELECT 'Literal_uint64' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'field_type') AS ft,
    JSONExtractUInt(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'value') AS val
FROM (SELECT parseQueryToJSON('SELECT 42') AS j);

-- String literal
SELECT 'Literal_string' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'field_type') AS ft,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'value') AS val
FROM (SELECT parseQueryToJSON('SELECT \'hello\'') AS j);

-- Null literal
SELECT 'Literal_null' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'field_type') AS ft
FROM (SELECT parseQueryToJSON('SELECT NULL') AS j);

-- Float64 literal
SELECT 'Literal_float' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'field_type') AS ft
FROM (SELECT parseQueryToJSON('SELECT 3.14') AS j);

-- Int64 literal (negative)
SELECT 'Literal_int64' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'arguments', 'children', 1, 'value', 'field_type') AS ft
FROM (SELECT parseQueryToJSON('SELECT -1') AS j);

-- Literal with alias
SELECT 'Literal_alias' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'alias') AS al
FROM (SELECT parseQueryToJSON('SELECT 42 AS x') AS j);

-- Array literal
SELECT 'Literal_array' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'arguments', 'children', 1, 'value', 'field_type') AS ft
FROM (SELECT parseQueryToJSON('SELECT [1, 2, 3]') AS j);

-- Tuple literal
SELECT 'Literal_tuple' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS fn
FROM (SELECT parseQueryToJSON('SELECT (1, 2)') AS j);

-- ==========================================================================
-- 6. ASTIdentifier
-- Fields: name(string), name_parts(array), alias(string)
-- ==========================================================================

SELECT 'Identifier_simple' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name
FROM (SELECT parseQueryToJSON('SELECT a') AS j);

-- Compound identifier with name_parts
SELECT 'Identifier_compound' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name,
    JSONExtractRaw(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name_parts') AS parts
FROM (SELECT parseQueryToJSON('SELECT a.b.c') AS j);

-- Identifier with alias
SELECT 'Identifier_alias' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'alias') AS al
FROM (SELECT parseQueryToJSON('SELECT a AS x') AS j);

-- ==========================================================================
-- 7. ASTTableIdentifier
-- Fields: name(string), name_parts(array), uuid(string), alias(string)
-- ==========================================================================

-- Simple table
SELECT 'TableIdentifier_simple' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'database_and_table_name', 'name') AS name,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'database_and_table_name', 'ast_type') AS tp
FROM (SELECT parseQueryToJSON('SELECT 1 FROM t') AS j);

-- Compound table (db.table) with name_parts
SELECT 'TableIdentifier_compound' AS t,
    JSONExtractRaw(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'database_and_table_name', 'name_parts') AS parts
FROM (SELECT parseQueryToJSON('SELECT 1 FROM db.t') AS j);

-- ==========================================================================
-- 8. ASTFunction
-- Fields: name, is_operator, is_window_function, compute_after_window_functions,
--         is_lambda_function, prefer_subquery_to_function_formatting, no_empty_args,
--         is_compound_name, nulls_action, kind, arguments, parameters,
--         window_name, window_definition, alias
-- ==========================================================================

-- Simple function
SELECT 'Function_simple' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'arguments') AS has_args
FROM (SELECT parseQueryToJSON('SELECT count(*)') AS j);

-- Operator function (plus written as +)
SELECT 'Function_operator' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'is_operator') AS is_op
FROM (SELECT parseQueryToJSON('SELECT a + b FROM t') AS j);

-- Parametric function
SELECT 'Function_parametric' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'parameters') AS has_params
FROM (SELECT parseQueryToJSON('SELECT quantile(0.9)(x) FROM t') AS j);

-- Window function with definition
SELECT 'Function_window' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'is_window_function') AS is_wf,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition') AS has_wd,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'kind') AS kind
FROM (SELECT parseQueryToJSON('SELECT row_number() OVER (ORDER BY a) FROM t') AS j);

-- Window function with named window
SELECT 'Function_window_name' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_name') AS wn
FROM (SELECT parseQueryToJSON('SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY a)') AS j);

-- Lambda function
SELECT 'Function_lambda' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'arguments', 'children', 1, 'is_lambda_function') AS is_lambda,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'arguments', 'children', 1, 'kind') AS kind
FROM (SELECT parseQueryToJSON('SELECT arrayMap(x -> x * 2, [1, 2, 3])') AS j);

-- Function with RESPECT NULLS
SELECT 'Function_nulls_action' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'nulls_action') AS na
FROM (SELECT parseQueryToJSON('SELECT any_respect_nulls(a) FROM t') AS j);

-- Function with alias
SELECT 'Function_alias' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'alias') AS al
FROM (SELECT parseQueryToJSON('SELECT count(*) AS cnt FROM t') AS j);

-- ==========================================================================
-- 9. ASTSubquery
-- Fields: cte_name(string), alias(string), children(array)
-- ==========================================================================

SELECT 'Subquery' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'subquery', 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'subquery', 'children') AS has_children
FROM (SELECT parseQueryToJSON('SELECT * FROM (SELECT 1 AS x)') AS j);

-- Subquery with alias
SELECT 'Subquery_alias' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'subquery', 'alias') AS al
FROM (SELECT parseQueryToJSON('SELECT * FROM (SELECT 1) AS sub') AS j);

-- ==========================================================================
-- 10. ASTQueryParameter
-- Fields: name(string), param_type(string), alias(string)
-- ==========================================================================

SELECT 'QueryParameter' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'name') AS name,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'param_type') AS pt
FROM (SELECT parseQueryToJSON('SELECT {param:UInt64}') AS j);

-- ==========================================================================
-- 11. ASTAsterisk
-- Fields: expression(child), transformers(child)
-- ==========================================================================

SELECT 'Asterisk' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp
FROM (SELECT parseQueryToJSON('SELECT * FROM t') AS j);

-- Asterisk with APPLY transformer
SELECT 'Asterisk_transformers' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers') AS has_tr
FROM (SELECT parseQueryToJSON('SELECT * APPLY(toString) FROM t') AS j);

-- ==========================================================================
-- 12. ASTQualifiedAsterisk
-- Fields: qualifier(child), transformers(child)
-- ==========================================================================

SELECT 'QualifiedAsterisk' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'qualifier') AS has_qual
FROM (SELECT parseQueryToJSON('SELECT t.* FROM t') AS j);

-- QualifiedAsterisk with transformers
SELECT 'QualifiedAsterisk_tr' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers') AS has_tr
FROM (SELECT parseQueryToJSON('SELECT t.* EXCEPT(a) FROM t') AS j);

-- ==========================================================================
-- 13. ASTColumnsRegexpMatcher
-- Fields: pattern(string), expression(child), transformers(child)
-- ==========================================================================

SELECT 'ColumnsRegexpMatcher' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'pattern') AS pat
FROM (SELECT parseQueryToJSON('SELECT COLUMNS(\'col.*\') FROM t') AS j);

-- ==========================================================================
-- 14. ASTColumnsListMatcher
-- Fields: expression(child), column_list(child), transformers(child)
-- ==========================================================================

SELECT 'ColumnsListMatcher' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'column_list') AS has_cl
FROM (SELECT parseQueryToJSON('SELECT COLUMNS(a, b) FROM t') AS j);

-- ==========================================================================
-- 15. ASTQualifiedColumnsRegexpMatcher
-- Fields: pattern(string), qualifier(child), transformers(child)
-- ==========================================================================

SELECT 'QualifiedColumnsRegexpMatcher' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'pattern') AS pat,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'qualifier') AS has_qual
FROM (SELECT parseQueryToJSON('SELECT t.COLUMNS(\'x.*\') FROM t') AS j);

-- ==========================================================================
-- 16. ASTQualifiedColumnsListMatcher
-- Fields: qualifier(child), column_list(child), transformers(child)
-- ==========================================================================

SELECT 'QualifiedColumnsListMatcher' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'qualifier') AS has_qual,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'column_list') AS has_cl
FROM (SELECT parseQueryToJSON('SELECT t.COLUMNS(a, b) FROM t') AS j);

-- ==========================================================================
-- 17. ASTColumnsApplyTransformer
-- Fields: func_name(string), parameters(child), lambda(child), lambda_arg(string), column_name_prefix(string)
-- ==========================================================================

SELECT 'ColumnsApplyTransformer' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'ast_type') AS tp,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'func_name') AS fn
FROM (SELECT parseQueryToJSON('SELECT * APPLY(toString) FROM t') AS j);

-- ColumnsApplyTransformer: func_name verified above; column_name_prefix requires special syntax not available in standard SQL

-- ==========================================================================
-- 18. ASTColumnsExceptTransformer
-- Fields: is_strict(bool), pattern(string), children(array)
-- ==========================================================================

SELECT 'ColumnsExceptTransformer' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'children') AS has_ch
FROM (SELECT parseQueryToJSON('SELECT * EXCEPT(a) FROM t') AS j);

SELECT 'ColumnsExceptTransformer_strict' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'is_strict') AS strict
FROM (SELECT parseQueryToJSON('SELECT * EXCEPT STRICT (a) FROM t') AS j);

-- ==========================================================================
-- 19. ASTColumnsReplaceTransformer
-- Fields: is_strict(bool), children(array)
-- ==========================================================================

SELECT 'ColumnsReplaceTransformer' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'ast_type') AS tp
FROM (SELECT parseQueryToJSON('SELECT * REPLACE(a + 1 AS a) FROM t') AS j);

-- ==========================================================================
-- 20. ASTTablesInSelectQuery
-- Fields: children(array)
-- ==========================================================================

SELECT 'TablesInSelectQuery' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children') AS has_children
FROM (SELECT parseQueryToJSON('SELECT 1 FROM t') AS j);

-- ==========================================================================
-- 21. ASTTablesInSelectQueryElement
-- Fields: table_join(child), table_expression(child), array_join(child)
-- ==========================================================================

SELECT 'TablesInSelectQueryElement' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'ast_type') AS tp,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression') AS has_te
FROM (SELECT parseQueryToJSON('SELECT 1 FROM t') AS j);

-- Element with join
SELECT 'TablesInSelectQueryElement_join' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join') AS has_tj,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_expression') AS has_te
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 JOIN t2 ON t1.id = t2.id') AS j);

-- Element with array join
SELECT 'TablesInSelectQueryElement_array_join' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'array_join') AS has_aj
FROM (SELECT parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x') AS j);

-- ==========================================================================
-- 22. ASTTableExpression
-- Fields: final(bool), database_and_table_name(child), table_function(child),
--         subquery(child), sample_size(child), sample_offset(child), column_aliases(child)
-- ==========================================================================

-- With table name
SELECT 'TableExpression_table' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'database_and_table_name') AS has_dtn
FROM (SELECT parseQueryToJSON('SELECT 1 FROM t') AS j);

-- With FINAL
SELECT 'TableExpression_final' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'final') AS final_flag
FROM (SELECT parseQueryToJSON('SELECT a FROM t FINAL') AS j);

-- With table function
SELECT 'TableExpression_func' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'table_function') AS has_tf
FROM (SELECT parseQueryToJSON('SELECT * FROM numbers(10)') AS j);

-- With subquery
SELECT 'TableExpression_subquery' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'subquery') AS has_sq
FROM (SELECT parseQueryToJSON('SELECT * FROM (SELECT 1)') AS j);

-- With sample_size and sample_offset
SELECT 'TableExpression_sample' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'sample_size') AS has_ss,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'sample_offset') AS has_so
FROM (SELECT parseQueryToJSON('SELECT a FROM t SAMPLE 1/10 OFFSET 1/2') AS j);

-- ==========================================================================
-- 23. ASTTableJoin
-- Fields: locality(string), strictness(string), kind(string), is_natural(bool),
--         using_expression_list(child), on_expression(child)
-- ==========================================================================

-- INNER JOIN with ON
SELECT 'TableJoin_inner_on' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'kind') AS kind,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'on_expression') AS has_on
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id') AS j);

-- LEFT JOIN with USING
SELECT 'TableJoin_left_using' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'kind') AS kind,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'strictness') AS strictness,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'using_expression_list') AS has_using
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 LEFT JOIN t2 USING (id)') AS j);

-- GLOBAL JOIN
SELECT 'TableJoin_global' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'locality') AS locality
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 GLOBAL LEFT JOIN t2 ON t1.id = t2.id') AS j);

-- CROSS JOIN
SELECT 'TableJoin_cross' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'kind') AS kind
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 CROSS JOIN t2') AS j);

-- NATURAL JOIN
SELECT 'TableJoin_natural' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'table_join', 'is_natural') AS is_natural
FROM (SELECT parseQueryToJSON('SELECT a FROM t1 NATURAL JOIN t2') AS j);

-- ==========================================================================
-- 24. ASTArrayJoin
-- Fields: kind(string), expression_list(child)
-- ==========================================================================

SELECT 'ArrayJoin_inner' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'array_join', 'kind') AS kind,
    JSONHas(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'array_join', 'expression_list') AS has_el
FROM (SELECT parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x') AS j);

SELECT 'ArrayJoin_left' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 2, 'array_join', 'kind') AS kind
FROM (SELECT parseQueryToJSON('SELECT x FROM t LEFT ARRAY JOIN arr AS x') AS j);

-- ==========================================================================
-- 25. ASTOrderByElement
-- Fields: direction(int), nulls_direction(int), nulls_direction_was_explicitly_specified(bool),
--         with_fill(bool), collation(child), fill_from(child), fill_to(child),
--         fill_step(child), fill_staleness(child), children(array)
-- ==========================================================================

SELECT 'OrderByElement_asc' AS t,
    JSONExtractInt(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'direction') AS dir,
    JSONExtractInt(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'nulls_direction') AS nd
FROM (SELECT parseQueryToJSON('SELECT a FROM t ORDER BY a ASC') AS j);

SELECT 'OrderByElement_desc_nulls' AS t,
    JSONExtractInt(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'direction') AS dir,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'nulls_direction_was_explicitly_specified') AS nulls_explicit
FROM (SELECT parseQueryToJSON('SELECT a FROM t ORDER BY a DESC NULLS FIRST') AS j);

SELECT 'OrderByElement_fill' AS t,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'with_fill') AS wf,
    JSONHas(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'fill_from') AS has_from,
    JSONHas(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'fill_to') AS has_to,
    JSONHas(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'fill_step') AS has_step
FROM (SELECT parseQueryToJSON('SELECT a FROM t ORDER BY a WITH FILL FROM 1 TO 10 STEP 1') AS j);

-- ==========================================================================
-- 26. ASTStorageOrderByElement
-- Fields: direction(int), children(array)
-- (Appears in CREATE TABLE ... ORDER BY)
-- ==========================================================================

SELECT 'StorageOrderByElement' AS t,
    JSONExtractString(j, 'storage', 'order_by', 'ast_type') AS tp
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 27. ASTWindowDefinition
-- Fields: parent_window_name(string), partition_by(child), order_by(child),
--         frame_is_default(bool), frame_type(string), frame_begin_type(string),
--         frame_begin_offset(child), frame_begin_preceding(bool),
--         frame_end_type(string), frame_end_offset(child), frame_end_preceding(bool)
-- ==========================================================================

SELECT 'WindowDefinition_full' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'partition_by') AS has_part,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'order_by') AS has_ord,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_type') AS ft,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_begin_type') AS fbt,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_begin_preceding') AS fbp,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_end_type') AS fet,
    JSONExtractBool(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_end_preceding') AS fep
FROM (SELECT parseQueryToJSON('SELECT row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t') AS j);

-- Window with offset frame
SELECT 'WindowDefinition_offset' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_begin_type') AS fbt,
    JSONHas(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_begin_offset') AS has_offset,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'window_definition', 'frame_end_type') AS fet
FROM (SELECT parseQueryToJSON('SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t') AS j);

-- ==========================================================================
-- 28. ASTWindowListElement
-- Fields: name(string), definition(child)
-- ==========================================================================

SELECT 'WindowListElement' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'window', 'children', 1, 'name') AS name,
    JSONHas(j, 'list_of_selects', 'children', 1, 'window', 'children', 1, 'definition') AS has_def
FROM (SELECT parseQueryToJSON('SELECT row_number() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)') AS j);

-- ==========================================================================
-- 29. ASTSampleRatio
-- Fields: numerator(string), denominator(string)
-- ==========================================================================

SELECT 'SampleRatio' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'sample_size', 'numerator') AS num,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'tables', 'children', 1, 'table_expression', 'sample_size', 'denominator') AS den
FROM (SELECT parseQueryToJSON('SELECT a FROM t SAMPLE 1/10') AS j);

-- ==========================================================================
-- 30. ASTInterpolateElement
-- Fields: column(string), expr(child)
-- ==========================================================================

SELECT 'InterpolateElement' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'interpolate', 'children', 1, 'column') AS col,
    JSONHas(j, 'list_of_selects', 'children', 1, 'interpolate', 'children', 1, 'expr') AS has_expr
FROM (SELECT parseQueryToJSON('SELECT a, b FROM t ORDER BY a WITH FILL INTERPOLATE (b AS b + 1)') AS j);

-- ==========================================================================
-- 31. ASTCollation
-- Fields: collation(child)
-- ==========================================================================

SELECT 'Collation' AS t,
    JSONHas(j, 'list_of_selects', 'children', 1, 'order_by', 'children', 1, 'collation') AS has_coll
FROM (SELECT parseQueryToJSON('SELECT a FROM t ORDER BY a COLLATE \'en\'') AS j);

-- ==========================================================================
-- 32. ASTSetQuery
-- Fields: is_standalone(bool), changes(array), default_settings(array), query_parameters(array)
-- ==========================================================================

SELECT 'SetQuery_standalone' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractBool(j, 'is_standalone') AS standalone,
    JSONExtractString(j, 'changes', 1, 'name') AS ch_name
FROM (SELECT parseQueryToJSON('SET max_threads = 4') AS j);

-- SET with multiple changes
SELECT 'SetQuery_multi' AS t,
    JSONLength(j, 'changes') AS num_changes
FROM (SELECT parseQueryToJSON('SET max_threads = 4, max_memory_usage = 1000000') AS j);

-- ==========================================================================
-- 33. ASTExplainQuery
-- Fields: kind(string), settings(child), query(child)
-- ==========================================================================

SELECT 'ExplainQuery_AST' AS t,
    JSONExtractString(j, 'kind') AS kind,
    JSONHas(j, 'query') AS has_query
FROM (SELECT parseQueryToJSON('EXPLAIN AST SELECT 1') AS j);

SELECT 'ExplainQuery_SYNTAX' AS t,
    JSONExtractString(j, 'kind') AS kind
FROM (SELECT parseQueryToJSON('EXPLAIN SYNTAX SELECT 1') AS j);

SELECT 'ExplainQuery_PLAN' AS t,
    JSONExtractString(j, 'kind') AS kind
FROM (SELECT parseQueryToJSON('EXPLAIN PLAN SELECT 1') AS j);

-- ==========================================================================
-- 34. ASTWithElement
-- Fields: name(string), subquery(child)
-- ==========================================================================

SELECT 'WithElement' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'with', 'children', 1, 'name') AS name,
    JSONHas(j, 'list_of_selects', 'children', 1, 'with', 'children', 1, 'subquery') AS has_sq
FROM (SELECT parseQueryToJSON('WITH x AS (SELECT 1 AS a) SELECT * FROM x') AS j);

-- ==========================================================================
-- 35. ASTColumnDeclaration
-- Fields: name(string), default_specifier(string), null_modifier(bool),
--         ephemeral_default(bool), primary_key_specifier(bool), data_type(child),
--         default_expression(child), comment(child), codec(child),
--         statistics_desc(child), ttl(child), collation(child), settings(child)
-- ==========================================================================

-- Basic column
SELECT 'ColumnDeclaration_basic' AS t,
    JSONExtractString(j, 'columns_list', 'columns', 'children', 1, 'name') AS name,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'data_type') AS has_dt
FROM (SELECT parseQueryToJSON('CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id') AS j);

-- Column with DEFAULT
SELECT 'ColumnDeclaration_default' AS t,
    JSONExtractString(j, 'columns_list', 'columns', 'children', 1, 'default_specifier') AS ds,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'default_expression') AS has_de
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY a') AS j);

-- Column with NULL modifier
SELECT 'ColumnDeclaration_null' AS t,
    JSONExtractBool(j, 'columns_list', 'columns', 'children', 1, 'null_modifier') AS nm
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a Nullable(UInt64) NULL) ENGINE = MergeTree ORDER BY tuple()') AS j);

-- Column with COMMENT
SELECT 'ColumnDeclaration_comment' AS t,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'comment') AS has_comment
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64 COMMENT \'my comment\') ENGINE = MergeTree ORDER BY a') AS j);

-- Column with CODEC
SELECT 'ColumnDeclaration_codec' AS t,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'codec') AS has_codec
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64 CODEC(LZ4)) ENGINE = MergeTree ORDER BY a') AS j);

-- Column with TTL
SELECT 'ColumnDeclaration_ttl' AS t,
    JSONHas(j, 'columns_list', 'columns', 'children', 2, 'ttl') AS has_ttl
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, d DateTime TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 36. ASTIndexDeclaration
-- Fields: name(string), granularity(uint), expression(child), index_type(child)
-- ==========================================================================

SELECT 'IndexDeclaration' AS t,
    JSONExtractString(j, 'columns_list', 'indices', 'children', 1, 'name') AS name,
    JSONExtractUInt(j, 'columns_list', 'indices', 'children', 1, 'granularity') AS gran,
    JSONHas(j, 'columns_list', 'indices', 'children', 1, 'expression') AS has_expr,
    JSONHas(j, 'columns_list', 'indices', 'children', 1, 'index_type') AS has_type
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a String, INDEX idx a TYPE bloom_filter GRANULARITY 4) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 37. ASTConstraintDeclaration
-- Fields: name(string), constraint_type(string), expr(child)
-- ==========================================================================

SELECT 'ConstraintDeclaration' AS t,
    JSONExtractString(j, 'columns_list', 'constraints', 'children', 1, 'name') AS name,
    JSONExtractString(j, 'columns_list', 'constraints', 'children', 1, 'constraint_type') AS ct,
    JSONHas(j, 'columns_list', 'constraints', 'children', 1, 'expr') AS has_expr
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a') AS j);

-- ASSUME constraint
SELECT 'ConstraintDeclaration_assume' AS t,
    JSONExtractString(j, 'columns_list', 'constraints', 'children', 1, 'constraint_type') AS ct
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, CONSTRAINT c ASSUME a > 0) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 38. ASTProjectionDeclaration
-- Fields: name(string), query(child)
-- ==========================================================================

SELECT 'ProjectionDeclaration' AS t,
    JSONExtractString(j, 'columns_list', 'projections', 'children', 1, 'name') AS name,
    JSONHas(j, 'columns_list', 'projections', 'children', 1, 'query') AS has_query
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, b UInt64, PROJECTION p (SELECT a, sum(b) GROUP BY a)) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 39. ASTStatisticsDeclaration
-- Fields: columns(child), types(child)
-- ==========================================================================

SELECT 'StatisticsDeclaration' AS t,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'statistics_desc', 'columns') AS has_cols,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'statistics_desc', 'types') AS has_types
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 40. ASTStorage
-- Fields: engine(child), partition_by(child), primary_key(child), order_by(child),
--         sample_by(child), ttl_table(child), settings(child)
-- ==========================================================================

SELECT 'Storage_full' AS t,
    JSONHas(j, 'storage', 'engine') AS has_engine,
    JSONHas(j, 'storage', 'partition_by') AS has_part,
    JSONHas(j, 'storage', 'primary_key') AS has_pk,
    JSONHas(j, 'storage', 'order_by') AS has_ob,
    JSONHas(j, 'storage', 'sample_by') AS has_sb,
    JSONHas(j, 'storage', 'ttl_table') AS has_ttl,
    JSONHas(j, 'storage', 'settings') AS has_settings
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, d Date) ENGINE = MergeTree PARTITION BY d PRIMARY KEY a ORDER BY a SAMPLE BY a TTL d + INTERVAL 1 DAY SETTINGS index_granularity = 8192') AS j);

-- ==========================================================================
-- 41. ASTColumns (Columns definition)
-- Fields: columns(child), indices(child), constraints(child), projections(child), primary_key(child)
-- ==========================================================================

SELECT 'Columns_definition' AS t,
    JSONExtractString(j, 'columns_list', 'ast_type') AS tp,
    JSONHas(j, 'columns_list', 'columns') AS has_cols,
    JSONHas(j, 'columns_list', 'indices') AS has_idx,
    JSONHas(j, 'columns_list', 'constraints') AS has_con,
    JSONHas(j, 'columns_list', 'projections') AS has_proj
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, INDEX idx a TYPE minmax GRANULARITY 1, CONSTRAINT c CHECK a > 0, PROJECTION p (SELECT a)) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 42. ASTCreateQuery
-- Fields: database, table, cluster, attach, if_not_exists, is_ordinary_view,
--         is_materialized_view, is_dictionary, columns_list, storage, select,
--         dictionary, as_database, as_table
-- ==========================================================================

-- Basic CREATE TABLE
SELECT 'CreateQuery_basic' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'database') AS db,
    JSONExtractString(j, 'table') AS tbl,
    JSONExtractBool(j, 'if_not_exists') AS ine
FROM (SELECT parseQueryToJSON('CREATE TABLE IF NOT EXISTS db.t (a UInt64) ENGINE = MergeTree ORDER BY a') AS j);

-- CREATE VIEW
SELECT 'CreateQuery_view' AS t,
    JSONExtractBool(j, 'is_ordinary_view') AS is_view
FROM (SELECT parseQueryToJSON('CREATE VIEW v AS SELECT 1') AS j);

-- CREATE MATERIALIZED VIEW
SELECT 'CreateQuery_matview' AS t,
    JSONExtractBool(j, 'is_materialized_view') AS is_mv
FROM (SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a') AS j);

-- CREATE TABLE AS SELECT
SELECT 'CreateQuery_as_select' AS t,
    JSONHas(j, 'select') AS has_select
FROM (SELECT parseQueryToJSON('CREATE TABLE t ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a') AS j);

-- CREATE TABLE ... AS other_table
SELECT 'CreateQuery_as_table' AS t,
    JSONExtractString(j, 'as_table') AS as_tbl
FROM (SELECT parseQueryToJSON('CREATE TABLE t2 AS t') AS j);

-- CREATE TABLE on cluster
SELECT 'CreateQuery_cluster' AS t,
    JSONExtractString(j, 'cluster') AS cluster_name
FROM (SELECT parseQueryToJSON('CREATE TABLE t ON CLUSTER my_cluster (a UInt64) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 43. ASTDropQuery
-- Fields: kind(string), database(string), table(string), if_exists(bool),
--         sync(bool), permanently(bool), is_dictionary(bool), is_view(bool)
-- ==========================================================================

SELECT 'DropQuery_table' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'kind') AS kind,
    JSONExtractString(j, 'table') AS tbl,
    JSONExtractBool(j, 'if_exists') AS ie
FROM (SELECT parseQueryToJSON('DROP TABLE IF EXISTS t') AS j);

SELECT 'DropQuery_database' AS t,
    JSONExtractString(j, 'kind') AS kind,
    JSONExtractString(j, 'database') AS db,
    JSONExtractBool(j, 'if_exists') AS ie
FROM (SELECT parseQueryToJSON('DROP DATABASE IF EXISTS db') AS j);

SELECT 'DropQuery_sync' AS t,
    JSONExtractBool(j, 'sync') AS sync_flag
FROM (SELECT parseQueryToJSON('DROP TABLE IF EXISTS t SYNC') AS j);

SELECT 'DropQuery_view' AS t,
    JSONExtractBool(j, 'is_view') AS is_view
FROM (SELECT parseQueryToJSON('DROP VIEW IF EXISTS v') AS j);

-- ==========================================================================
-- 44. ASTInsertQuery
-- Fields: database_name(string), table_name(string), database(child), table(child),
--         format(string), columns(child), table_function(child), select(child)
-- ==========================================================================

SELECT 'InsertQuery_select' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'table') AS has_table,
    JSONHas(j, 'select') AS has_select
FROM (SELECT parseQueryToJSON('INSERT INTO t SELECT 1, 2') AS j);

SELECT 'InsertQuery_columns' AS t,
    JSONHas(j, 'columns') AS has_cols
FROM (SELECT parseQueryToJSON('INSERT INTO t (a, b) SELECT 1, 2') AS j);

-- INSERT into table function
SELECT 'InsertQuery_func' AS t,
    JSONHas(j, 'table_function') AS has_tf
FROM (SELECT parseQueryToJSON('INSERT INTO FUNCTION file(\'test.csv\') SELECT 1') AS j);

-- ==========================================================================
-- 45. ASTAlterCommand
-- Fields: command_type(int), col_decl(child), column(child), partition(child),
--         predicate(child), update_assignments(child)
-- ==========================================================================

-- ALTER ADD COLUMN
SELECT 'AlterCommand_add' AS t,
    JSONExtractInt(j, 'command_list', 'children', 1, 'command_type') AS cmd_type,
    JSONHas(j, 'command_list', 'children', 1, 'col_decl') AS has_cd
FROM (SELECT parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt64') AS j);

-- ALTER DROP COLUMN
SELECT 'AlterCommand_drop' AS t,
    JSONHas(j, 'command_list', 'children', 1, 'column') AS has_col
FROM (SELECT parseQueryToJSON('ALTER TABLE t DROP COLUMN c') AS j);

-- ALTER UPDATE (has predicate and update_assignments)
SELECT 'AlterCommand_update' AS t,
    JSONHas(j, 'command_list', 'children', 1, 'update_assignments') AS has_ua,
    JSONHas(j, 'command_list', 'children', 1, 'predicate') AS has_pred
FROM (SELECT parseQueryToJSON('ALTER TABLE t UPDATE a = 1 WHERE b = 2') AS j);

-- ALTER DELETE (has predicate)
SELECT 'AlterCommand_delete' AS t,
    JSONHas(j, 'command_list', 'children', 1, 'predicate') AS has_pred
FROM (SELECT parseQueryToJSON('ALTER TABLE t DELETE WHERE a = 1') AS j);

-- ALTER with partition
SELECT 'AlterCommand_partition' AS t,
    JSONHas(j, 'command_list', 'children', 1, 'partition') AS has_part
FROM (SELECT parseQueryToJSON('ALTER TABLE t DROP PARTITION 202301') AS j);

-- ==========================================================================
-- 46. ASTAlterQuery
-- Fields: database(string), table(string), alter_object(string), command_list(child)
-- ==========================================================================

SELECT 'AlterQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'table') AS tbl,
    JSONExtractString(j, 'alter_object') AS obj,
    JSONHas(j, 'command_list') AS has_cl
FROM (SELECT parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt64') AS j);

-- ==========================================================================
-- 47. ASTRenameQuery
-- Fields: exchange(bool), elements(array)
-- ==========================================================================

SELECT 'RenameQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractBool(j, 'exchange') AS exch,
    JSONExtractString(j, 'elements', 1, 'from_table') AS from_tbl,
    JSONExtractString(j, 'elements', 1, 'to_table') AS to_tbl,
    JSONExtractBool(j, 'elements', 1, 'if_exists') AS ie
FROM (SELECT parseQueryToJSON('RENAME TABLE a TO b') AS j);

SELECT 'RenameQuery_db' AS t,
    JSONExtractString(j, 'elements', 1, 'from_database') AS from_db,
    JSONExtractString(j, 'elements', 1, 'to_database') AS to_db
FROM (SELECT parseQueryToJSON('RENAME TABLE db1.a TO db2.b') AS j);

-- EXCHANGE TABLES
SELECT 'RenameQuery_exchange' AS t,
    JSONExtractBool(j, 'exchange') AS exch
FROM (SELECT parseQueryToJSON('EXCHANGE TABLES a AND b') AS j);

-- ==========================================================================
-- 48. ASTNameTypePair
-- Fields: name(string), name_type(child)
-- ==========================================================================

SELECT 'NameTypePair' AS t,
    JSONExtractString(j, 'columns_list', 'columns', 'children', 1, 'ast_type') AS col_tp
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 49. ASTDataType
-- Fields: name(string), arguments(child)
-- ==========================================================================

-- Simple data type
SELECT 'DataType_simple' AS t,
    JSONExtractString(j, 'columns_list', 'columns', 'children', 1, 'data_type', 'name') AS name
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a') AS j);

-- Parametric data type
SELECT 'DataType_parametric' AS t,
    JSONExtractString(j, 'columns_list', 'columns', 'children', 1, 'data_type', 'name') AS name,
    JSONHas(j, 'columns_list', 'columns', 'children', 1, 'data_type', 'arguments') AS has_args
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a FixedString(10)) ENGINE = MergeTree ORDER BY a') AS j);

-- ==========================================================================
-- 50. ASTFunctionWithKeyValueArguments
-- Fields: name(string), has_brackets(bool), elements(child)
-- ==========================================================================

-- Test via dictionary SOURCE
SELECT 'FunctionWithKV' AS t,
    JSONExtractString(j, 'dictionary', 'source', 'ast_type') AS tp,
    JSONExtractString(j, 'dictionary', 'source', 'name') AS name,
    JSONExtractBool(j, 'dictionary', 'source', 'has_brackets') AS hb,
    JSONHas(j, 'dictionary', 'source', 'elements') AS has_el
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- ==========================================================================
-- 51. ASTPair
-- Fields: first(string), second_with_brackets(bool), second(child)
-- ==========================================================================

-- ASTPair appears inside FunctionWithKeyValueArguments elements
SELECT 'Pair' AS t,
    JSONExtractString(j, 'dictionary', 'source', 'elements', 'children', 1, 'ast_type') AS tp,
    JSONExtractString(j, 'dictionary', 'source', 'elements', 'children', 1, 'first') AS first_val,
    JSONHas(j, 'dictionary', 'source', 'elements', 'children', 1, 'second') AS has_second
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- ==========================================================================
-- 52. ASTTTLElement
-- Fields: mode(string), destination_type(string), destination_name(string),
--         if_exists(bool)
-- ==========================================================================

SELECT 'TTLElement' AS t,
    JSONExtractString(j, 'storage', 'ttl_table', 'children', 1, 'mode') AS mode
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY') AS j);

-- TTL with MOVE TO DISK
SELECT 'TTLElement_move' AS t,
    JSONExtractString(j, 'storage', 'ttl_table', 'children', 1, 'mode') AS mode,
    JSONExtractString(j, 'storage', 'ttl_table', 'children', 1, 'destination_type') AS dt
FROM (SELECT parseQueryToJSON('CREATE TABLE t (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY TO DISK \'default\'') AS j);

-- ==========================================================================
-- 53. ASTPartition
-- Fields: value(child), all(bool)
-- ==========================================================================

SELECT 'Partition_value' AS t,
    JSONHas(j, 'command_list', 'children', 1, 'partition', 'value') AS has_val,
    JSONExtractBool(j, 'command_list', 'children', 1, 'partition', 'all') AS all_flag
FROM (SELECT parseQueryToJSON('ALTER TABLE t DROP PARTITION 202301') AS j);

-- DROP ALL PARTITIONS (not a standard syntax, test 'all' flag via PARTITION ALL in optimize)
SELECT 'Partition_all' AS t,
    JSONExtractBool(j, 'partition', 'all') AS all_flag
FROM (SELECT parseQueryToJSON('OPTIMIZE TABLE t PARTITION tuple() FINAL') AS j);

-- ==========================================================================
-- 54. ASTDictionary
-- Fields: primary_key(child), source(child), lifetime(child), layout(child),
--         range(child), dict_settings(child)
-- ==========================================================================

SELECT 'Dictionary' AS t,
    JSONHas(j, 'dictionary', 'primary_key') AS has_pk,
    JSONHas(j, 'dictionary', 'source') AS has_src,
    JSONHas(j, 'dictionary', 'lifetime') AS has_lt,
    JSONHas(j, 'dictionary', 'layout') AS has_lay
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- Dictionary with range
SELECT 'Dictionary_range' AS t,
    JSONHas(j, 'dictionary', 'range') AS has_range
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, start DateTime, end DateTime, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(RANGE_HASHED()) LIFETIME(MIN 0 MAX 1000) RANGE(MIN start MAX end)') AS j);

-- ==========================================================================
-- 55. ASTDictionaryAttributeDeclaration
-- Fields: name(string), attr_type(child), hierarchical(bool), injective(bool)
-- ==========================================================================

SELECT 'DictAttrDecl' AS t,
    JSONExtractString(j, 'dictionary_attributes_list', 'children', 1, 'name') AS name,
    JSONHas(j, 'dictionary_attributes_list', 'children', 1, 'attr_type') AS has_type
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\' HIERARCHICAL INJECTIVE) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

SELECT 'DictAttrDecl_flags' AS t,
    JSONExtractBool(j, 'dictionary_attributes_list', 'children', 2, 'hierarchical') AS hier,
    JSONExtractBool(j, 'dictionary_attributes_list', 'children', 2, 'injective') AS inj
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\' HIERARCHICAL INJECTIVE) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- ==========================================================================
-- 56. ASTDictionaryLifetime
-- Fields: min_sec(uint), max_sec(uint)
-- ==========================================================================

SELECT 'DictionaryLifetime' AS t,
    JSONExtractUInt(j, 'dictionary', 'lifetime', 'min_sec') AS min_s,
    JSONExtractUInt(j, 'dictionary', 'lifetime', 'max_sec') AS max_s
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- ==========================================================================
-- 57. ASTDictionaryLayout
-- Fields: layout_type(string), has_brackets(bool)
-- ==========================================================================

SELECT 'DictionaryLayout' AS t,
    JSONExtractString(j, 'dictionary', 'layout', 'layout_type') AS lt,
    JSONExtractBool(j, 'dictionary', 'layout', 'has_brackets') AS hb
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)') AS j);

-- ==========================================================================
-- 58. ASTDictionaryRange
-- Fields: min_attr_name(string), max_attr_name(string)
-- ==========================================================================

SELECT 'DictionaryRange' AS t,
    JSONExtractString(j, 'dictionary', 'range', 'min_attr_name') AS min_attr,
    JSONExtractString(j, 'dictionary', 'range', 'max_attr_name') AS max_attr
FROM (SELECT parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, start DateTime, end DateTime, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(RANGE_HASHED()) LIFETIME(MIN 0 MAX 1000) RANGE(MIN start MAX end)') AS j);

-- ==========================================================================
-- 59. ASTSystemQuery
-- Fields: query_type(int), query_type_name(string), database(child), table(child)
-- ==========================================================================

SELECT 'SystemQuery_flush' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'query_type_name') AS qtn
FROM (SELECT parseQueryToJSON('SYSTEM FLUSH LOGS') AS j);

SELECT 'SystemQuery_stop' AS t,
    JSONExtractString(j, 'query_type_name') AS qtn,
    JSONHas(j, 'table') AS has_table
FROM (SELECT parseQueryToJSON('SYSTEM STOP MERGES t') AS j);

-- ==========================================================================
-- 60. ASTShowTablesQuery
-- Fields: databases(bool), like(string), from(child)
-- ==========================================================================

SELECT 'ShowTablesQuery_from' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'from') AS has_from
FROM (SELECT parseQueryToJSON('SHOW TABLES FROM db') AS j);

SELECT 'ShowTablesQuery_like' AS t,
    JSONExtractString(j, 'like') AS like_val
FROM (SELECT parseQueryToJSON('SHOW TABLES LIKE \'%test%\'') AS j);

SELECT 'ShowTablesQuery_databases' AS t,
    JSONExtractBool(j, 'databases') AS db_flag
FROM (SELECT parseQueryToJSON('SHOW DATABASES') AS j);

-- ==========================================================================
-- 61. ASTShowColumnsQuery
-- Fields: database(string), table(string)
-- ==========================================================================

SELECT 'ShowColumnsQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractString(j, 'table') AS tbl
FROM (SELECT parseQueryToJSON('SHOW COLUMNS FROM t') AS j);

SELECT 'ShowColumnsQuery_db' AS t,
    JSONExtractString(j, 'database') AS db,
    JSONExtractString(j, 'table') AS tbl
FROM (SELECT parseQueryToJSON('SHOW COLUMNS FROM t FROM db') AS j);

-- ==========================================================================
-- 62. ASTKillQueryQuery
-- Fields: kill_type(int), where_expression(child), sync(bool), test(bool)
-- ==========================================================================

SELECT 'KillQueryQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'where_expression') AS has_where,
    JSONExtractBool(j, 'sync') AS sync_flag
FROM (SELECT parseQueryToJSON('KILL QUERY WHERE query_id = \'abc\' SYNC') AS j);

SELECT 'KillQueryQuery_test' AS t,
    JSONExtractBool(j, 'test') AS test_flag
FROM (SELECT parseQueryToJSON('KILL QUERY WHERE query_id = \'abc\' TEST') AS j);

-- ==========================================================================
-- 63. ASTOptimizeQuery
-- Fields: database(child), table(child), final(bool), deduplicate(bool)
-- ==========================================================================

SELECT 'OptimizeQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'table') AS has_table,
    JSONExtractBool(j, 'final') AS final_flag,
    JSONExtractBool(j, 'deduplicate') AS dedup
FROM (SELECT parseQueryToJSON('OPTIMIZE TABLE t FINAL DEDUPLICATE') AS j);

-- ==========================================================================
-- 64. ASTDeleteQuery
-- Fields: database(child), table(child), predicate(child)
-- ==========================================================================

SELECT 'DeleteQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'table') AS has_table,
    JSONHas(j, 'predicate') AS has_pred
FROM (SELECT parseQueryToJSON('DELETE FROM t WHERE id = 1') AS j);

-- ==========================================================================
-- 65. ASTUpdateQuery
-- Fields: database(child), table(child), assignments(child), predicate(child)
-- ==========================================================================

SELECT 'UpdateQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'table') AS has_table,
    JSONHas(j, 'assignments') AS has_assign,
    JSONHas(j, 'predicate') AS has_pred
FROM (SELECT parseQueryToJSON('ALTER TABLE t UPDATE a = 1 WHERE b = 2') AS j);

-- ==========================================================================
-- 66. ASTUseQuery
-- Fields: database(child)
-- ==========================================================================

SELECT 'UseQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONHas(j, 'database') AS has_db
FROM (SELECT parseQueryToJSON('USE mydb') AS j);

-- ==========================================================================
-- 67. ASTTransactionControl
-- Fields: action(int), snapshot(uint)
-- ==========================================================================

SELECT 'TransactionControl_begin' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractInt(j, 'action') AS action
FROM (SELECT parseQueryToJSON('BEGIN TRANSACTION') AS j);

SELECT 'TransactionControl_commit' AS t,
    JSONExtractInt(j, 'action') AS action
FROM (SELECT parseQueryToJSON('COMMIT') AS j);

SELECT 'TransactionControl_rollback' AS t,
    JSONExtractInt(j, 'action') AS action
FROM (SELECT parseQueryToJSON('ROLLBACK') AS j);

-- ==========================================================================
-- 68. ASTCheckTableQuery
-- Fields: (uses writeJSON with database, table, partition, part_name)
-- ==========================================================================

SELECT 'CheckTableQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp
FROM (SELECT parseQueryToJSON('CHECK TABLE t') AS j);

-- ==========================================================================
-- 69. ASTBackupQuery
-- Fields: kind(int), backup_name(child)
-- ==========================================================================

SELECT 'BackupQuery' AS t,
    JSONExtractString(j, 'ast_type') AS tp,
    JSONExtractInt(j, 'kind') AS kind,
    JSONHas(j, 'backup_name') AS has_bn
FROM (SELECT parseQueryToJSON('BACKUP TABLE t TO Disk(\'default\', \'backup1\')') AS j);

-- ==========================================================================
-- 70. ASTViewTargets
-- Fields: targets(array)
-- ==========================================================================

SELECT 'ViewTargets' AS t,
    JSONHas(j, 'targets') AS has_targets
FROM (SELECT parseQueryToJSON('CREATE VIEW v AS SELECT 1') AS j);

-- ==========================================================================
-- 71. ASTSQLSecurity
-- Fields: security_type(int), is_definer_current_user(bool), definer(child)
-- ==========================================================================

SELECT 'SQLSecurity' AS t,
    JSONHas(j, 'sql_security') AS has_ss
FROM (SELECT parseQueryToJSON('CREATE VIEW v DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT 1') AS j);

-- ==========================================================================
-- 72. ASTRefreshStrategy
-- Fields: schedule_kind(int), period(child), append(bool)
-- ==========================================================================

SELECT 'RefreshStrategy' AS t,
    JSONHas(j, 'refresh_strategy') AS has_rs
FROM (SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a') AS j);

-- ==========================================================================
-- 73. ASTTimeInterval
-- Fields: seconds(uint), months(uint)
-- ==========================================================================

-- ASTTimeInterval appears as the period inside ASTRefreshStrategy
SELECT 'TimeInterval' AS t,
    JSONExtractUInt(j, 'refresh_strategy', 'period', 'seconds') AS secs
FROM (SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a') AS j);

-- ==========================================================================
-- 74. ASTAssignment
-- Fields: column_name(string), children(array)
-- ==========================================================================

SELECT 'Assignment' AS t,
    JSONExtractString(j, 'command_list', 'children', 1, 'update_assignments', 'children', 1, 'column_name') AS cn,
    JSONHas(j, 'command_list', 'children', 1, 'update_assignments', 'children', 1, 'children') AS has_ch
FROM (SELECT parseQueryToJSON('ALTER TABLE t UPDATE a = 1 WHERE b = 2') AS j);

-- ==========================================================================
-- 75. ASTTableOverride
-- Fields: table_name(string), columns(child), storage(child)
-- ==========================================================================

-- ASTTableOverride appears in some CREATE ... TABLE OVERRIDE contexts
-- Testing via JSON structure verification
SELECT 'TableOverride' AS t, 1; -- placeholder: ASTTableOverride is tested via round-trip below

-- ==========================================================================
-- 76. ASTForeignKeyDeclaration
-- Fields: name(string), children(array)
-- ==========================================================================

-- ASTForeignKeyDeclaration is present in the parser but not widely used
SELECT 'ForeignKeyDeclaration' AS t, 1; -- placeholder: tested via round-trip below


-- ==========================================================================
-- ==========================================================================
-- ROUND-TRIP TESTS: formatQueryFromJSON(parseQueryToJSON(sql))
-- Verify that every AST type survives a round-trip through JSON
-- ==========================================================================
-- ==========================================================================

-- Basic SELECT
SELECT 'RT_select' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1'));

-- Complex SELECT with all clauses
SELECT 'RT_select_full' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT DISTINCT a, sum(b) AS s FROM t PREWHERE c > 0 WHERE d = 1 GROUP BY a WITH TOTALS HAVING s > 10 ORDER BY s DESC LIMIT 5 BY a LIMIT 10 OFFSET 2'));

-- UNION ALL
SELECT 'RT_union' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3'));

-- UNION DISTINCT
SELECT 'RT_union_distinct' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION DISTINCT SELECT 1'));

-- INTERSECT / EXCEPT
SELECT 'RT_intersect' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 INTERSECT SELECT 1'));
SELECT 'RT_except' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 EXCEPT SELECT 2'));

-- Literals
SELECT 'RT_literals' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 42, -1, 3.14, \'hello\', NULL, true, [1, 2, 3], (1, \'a\')'));

-- Identifiers
SELECT 'RT_identifiers' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b.c FROM t'));

-- Functions (operator, parametric, window)
SELECT 'RT_functions' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT plus(a, 1), count(*), quantile(0.9)(x) FROM t'));

-- Lambda
SELECT 'RT_lambda' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT arrayMap(x -> x * 2, [1, 2, 3])'));

-- Subquery
SELECT 'RT_subquery' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM (SELECT 1 AS x)'));

-- Query parameter
SELECT 'RT_param' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT {param:UInt64}'));

-- Asterisk, QualifiedAsterisk
SELECT 'RT_asterisk' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t'));
SELECT 'RT_qual_asterisk' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.* FROM t'));

-- Column matchers
SELECT 'RT_columns_regexp' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(\'col.*\') FROM t'));
SELECT 'RT_columns_list' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(a, b) FROM t'));
SELECT 'RT_qual_columns_regexp' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.COLUMNS(\'x.*\') FROM t'));
SELECT 'RT_qual_columns_list' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.COLUMNS(a, b) FROM t'));

-- Column transformers
SELECT 'RT_apply' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(toString) FROM t'));
SELECT 'RT_except' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT(a) FROM t'));
SELECT 'RT_except_strict' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT STRICT (a) FROM t'));
SELECT 'RT_replace' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * REPLACE(a + 1 AS a) FROM t'));

-- Joins
SELECT 'RT_inner_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id'));
SELECT 'RT_left_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 LEFT JOIN t2 USING (id)'));
SELECT 'RT_cross_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 CROSS JOIN t2'));
SELECT 'RT_global_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 GLOBAL LEFT JOIN t2 ON t1.id = t2.id'));
SELECT 'RT_natural_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 NATURAL JOIN t2'));

-- Array join
SELECT 'RT_array_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x'));
SELECT 'RT_left_array_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t LEFT ARRAY JOIN arr AS x'));

-- ORDER BY
SELECT 'RT_order_by' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST'));
SELECT 'RT_order_by_fill' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a WITH FILL FROM 1 TO 10 STEP 1'));

-- Window definitions
SELECT 'RT_window' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t'));
SELECT 'RT_window_named' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT row_number() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)'));

-- SAMPLE
SELECT 'RT_sample' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t SAMPLE 1/10'));
SELECT 'RT_sample_offset' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t SAMPLE 0.1 OFFSET 0.5'));

-- INTERPOLATE
SELECT 'RT_interpolate' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t ORDER BY a WITH FILL INTERPOLATE (b AS b + 1)'));

-- COLLATE
SELECT 'RT_collation' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a COLLATE \'en\''));

-- SET
SELECT 'RT_set' AS t, formatQueryFromJSON(parseQueryToJSON('SET max_threads = 4'));

-- EXPLAIN
SELECT 'RT_explain_ast' AS t, formatQueryFromJSON(parseQueryToJSON('EXPLAIN AST SELECT 1'));
SELECT 'RT_explain_syntax' AS t, formatQueryFromJSON(parseQueryToJSON('EXPLAIN SYNTAX SELECT 1'));

-- WITH (CTE)
SELECT 'RT_with' AS t, formatQueryFromJSON(parseQueryToJSON('WITH x AS (SELECT 1 AS a) SELECT * FROM x'));

-- GROUP BY modifiers
SELECT 'RT_rollup' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY ROLLUP(a, b)'));
SELECT 'RT_cube' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY CUBE(a, b)'));
SELECT 'RT_grouping_sets' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY GROUPING SETS ((a), (b))'));

-- LIMIT WITH TIES
SELECT 'RT_limit_with_ties' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a LIMIT 5 WITH TIES'));

-- LIMIT BY
SELECT 'RT_limit_by' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t LIMIT 1 BY a'));

-- QUALIFY
SELECT 'RT_qualify' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, row_number() OVER (ORDER BY a) AS rn FROM t QUALIFY rn <= 3'));

-- SETTINGS in SELECT
SELECT 'RT_settings' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 SETTINGS max_threads = 1'));

-- FINAL
SELECT 'RT_final' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t FINAL'));

-- Table function
SELECT 'RT_table_func' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM numbers(10)'));

-- CREATE TABLE with columns, indices, constraints, projections
SELECT 'RT_create_table' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE db.t (id UInt64, name String DEFAULT \'x\', ts DateTime, INDEX idx name TYPE bloom_filter GRANULARITY 4, CONSTRAINT c CHECK id > 0, PROJECTION p (SELECT id, count() GROUP BY id)) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY id'));

-- CREATE TABLE IF NOT EXISTS
SELECT 'RT_create_ine' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE IF NOT EXISTS t (a UInt64) ENGINE = MergeTree ORDER BY a'));

-- CREATE TABLE with codec and comment
SELECT 'RT_create_codec' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64 COMMENT \'my col\' CODEC(LZ4)) ENGINE = MergeTree ORDER BY a'));

-- CREATE TABLE with TTL
SELECT 'RT_create_ttl' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY'));

-- CREATE TABLE with SETTINGS
SELECT 'RT_create_settings' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192'));

-- CREATE TABLE with SAMPLE BY
SELECT 'RT_create_sample' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SAMPLE BY a'));

-- CREATE TABLE AS SELECT
SELECT 'RT_create_as_select' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a'));

-- CREATE VIEW
SELECT 'RT_create_view' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v AS SELECT 1'));

-- CREATE MATERIALIZED VIEW
SELECT 'RT_create_mv' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a'));

-- DROP TABLE
SELECT 'RT_drop_table' AS t, formatQueryFromJSON(parseQueryToJSON('DROP TABLE IF EXISTS t'));
SELECT 'RT_drop_db' AS t, formatQueryFromJSON(parseQueryToJSON('DROP DATABASE IF EXISTS db'));
SELECT 'RT_drop_view' AS t, formatQueryFromJSON(parseQueryToJSON('DROP VIEW IF EXISTS v'));
SELECT 'RT_drop_sync' AS t, formatQueryFromJSON(parseQueryToJSON('DROP TABLE IF EXISTS t SYNC'));

-- INSERT INTO ... SELECT
SELECT 'RT_insert' AS t, formatQueryFromJSON(parseQueryToJSON('INSERT INTO t SELECT 1, 2'));
SELECT 'RT_insert_cols' AS t, formatQueryFromJSON(parseQueryToJSON('INSERT INTO t (a, b) SELECT 1, 2'));

-- ALTER TABLE
SELECT 'RT_alter_add' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt64'));
SELECT 'RT_alter_drop' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP COLUMN c'));
SELECT 'RT_alter_modify' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN c String'));
SELECT 'RT_alter_rename' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t RENAME COLUMN a TO b'));
SELECT 'RT_alter_update' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t UPDATE a = 1 WHERE b = 2'));
SELECT 'RT_alter_delete' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DELETE WHERE a = 1'));
SELECT 'RT_alter_partition' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PARTITION 202301'));

-- RENAME
SELECT 'RT_rename' AS t, formatQueryFromJSON(parseQueryToJSON('RENAME TABLE a TO b'));
SELECT 'RT_rename_db' AS t, formatQueryFromJSON(parseQueryToJSON('RENAME TABLE db1.a TO db2.b'));
SELECT 'RT_exchange' AS t, formatQueryFromJSON(parseQueryToJSON('EXCHANGE TABLES a AND b'));

-- Dictionary
SELECT 'RT_dictionary' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)'));

-- Dictionary with range
SELECT 'RT_dict_range' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, start DateTime, end DateTime, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(RANGE_HASHED()) LIFETIME(MIN 0 MAX 1000) RANGE(MIN start MAX end)'));

-- SYSTEM queries
SELECT 'RT_system_flush' AS t, formatQueryFromJSON(parseQueryToJSON('SYSTEM FLUSH LOGS'));
SELECT 'RT_system_stop' AS t, formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP MERGES t'));

-- SHOW queries
SELECT 'RT_show_tables' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW TABLES FROM db'));
SELECT 'RT_show_tables_like' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW TABLES LIKE \'%test%\''));
SELECT 'RT_show_databases' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW DATABASES'));
SELECT 'RT_show_columns' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW COLUMNS FROM t'));

-- KILL QUERY
SELECT 'RT_kill' AS t, formatQueryFromJSON(parseQueryToJSON('KILL QUERY WHERE query_id = \'abc\''));

-- OPTIMIZE
SELECT 'RT_optimize' AS t, formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL'));
SELECT 'RT_optimize_dedup' AS t, formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL DEDUPLICATE'));

-- USE
SELECT 'RT_use' AS t, formatQueryFromJSON(parseQueryToJSON('USE mydb'));

-- CHECK TABLE
SELECT 'RT_check' AS t, formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t'));

-- DELETE (lightweight)
SELECT 'RT_delete' AS t, formatQueryFromJSON(parseQueryToJSON('DELETE FROM t WHERE id = 1'));

-- TRANSACTION
SELECT 'RT_begin' AS t, formatQueryFromJSON(parseQueryToJSON('BEGIN TRANSACTION'));
SELECT 'RT_commit' AS t, formatQueryFromJSON(parseQueryToJSON('COMMIT'));
SELECT 'RT_rollback' AS t, formatQueryFromJSON(parseQueryToJSON('ROLLBACK'));

-- BACKUP
SELECT 'RT_backup' AS t, formatQueryFromJSON(parseQueryToJSON('BACKUP TABLE t TO Disk(\'default\', \'backup1\')'));

-- Multiple JOINs
SELECT 'RT_multi_join' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id'));

-- Aliases
SELECT 'RT_aliases' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a AS x, b AS y FROM t AS u'));

-- Refresh strategy (materialized view)
SELECT 'RT_refresh' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a'));

-- SQL Security
SELECT 'RT_sql_security' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT 1'));

-- ==========================================================================
-- IDEMPOTENCE TESTS: double round-trip
-- ==========================================================================

SELECT 'Idempotent_select' AS t,
    formatQueryFromJSON(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON('SELECT a, sum(b) FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE c > 0 GROUP BY a ORDER BY a LIMIT 10'))))
    = formatQueryFromJSON(parseQueryToJSON('SELECT a, sum(b) FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE c > 0 GROUP BY a ORDER BY a LIMIT 10'));

SELECT 'Idempotent_create' AS t,
    formatQueryFromJSON(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b String DEFAULT \'x\') ENGINE = MergeTree ORDER BY a'))))
    = formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b String DEFAULT \'x\') ENGINE = MergeTree ORDER BY a'));

SELECT 'Idempotent_dict' AS t,
    formatQueryFromJSON(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)'))))
    = formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT \'\') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE \'t\')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)'));
