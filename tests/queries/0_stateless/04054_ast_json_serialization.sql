-- ==========================================================================
-- Test ALL AST types for writeJSON/readJSON round-trip
-- Each test: formatQueryFromJSON(parseQueryToJSON(sql)) should produce valid SQL
-- ==========================================================================

-- === Expression types ===

-- ExpressionList, Literal (UInt64, Int64, Float64, String, Null, Bool, Array, Tuple)
SELECT 'Literal_types' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 42, -1, 3.14, ''hello'', NULL, true, [1,2,3], (1, ''a'')'));

-- Identifier
SELECT 'Identifier' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b.c FROM t'));

-- TableIdentifier (via FROM)
SELECT 'TableIdentifier' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 FROM db.t'));

-- Function (ordinary, operator, parametric)
SELECT 'Function' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT plus(a, 1), count(*), quantile(0.9)(x) FROM t'));

-- Subquery
SELECT 'Subquery' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM (SELECT 1 AS x)'));

-- QueryParameter
SELECT 'QueryParameter' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT {param:UInt64}'));

-- Asterisk
SELECT 'Asterisk' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t'));

-- QualifiedAsterisk
SELECT 'QualifiedAsterisk' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.* FROM t'));

-- === Column Matchers & Transformers ===

-- ColumnsRegexpMatcher
SELECT 'ColumnsRegexpMatcher' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(''col.*'') FROM t'));

-- ColumnsListMatcher
SELECT 'ColumnsListMatcher' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(a, b) FROM t'));

-- ColumnsApplyTransformer
SELECT 'ColumnsApplyTransformer' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(toString) FROM t'));

-- ColumnsExceptTransformer
SELECT 'ColumnsExceptTransformer' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * EXCEPT(a) FROM t'));

-- ColumnsReplaceTransformer
SELECT 'ColumnsReplaceTransformer' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * REPLACE(a + 1 AS a) FROM t'));

-- QualifiedColumnsRegexpMatcher
SELECT 'QualifiedColumnsRegexpMatcher' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.COLUMNS(''x.*'') FROM t'));

-- QualifiedColumnsListMatcher
SELECT 'QualifiedColumnsListMatcher' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT t.COLUMNS(a, b) FROM t'));

-- === Query types ===

-- SelectQuery (with all clauses)
SELECT 'SelectQuery_full' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT DISTINCT a, sum(b) AS s FROM t PREWHERE c > 0 WHERE d = 1 GROUP BY a WITH TOTALS HAVING s > 10 ORDER BY s DESC LIMIT 5 BY a LIMIT 10 OFFSET 2'));

-- SelectWithUnionQuery
SELECT 'SelectWithUnionQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3'));

-- SelectIntersectExceptQuery
SELECT 'SelectIntersectExceptQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 INTERSECT SELECT 1'));

-- SelectIntersectExceptQuery (EXCEPT)
SELECT 'SelectExceptQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 EXCEPT SELECT 2'));

-- WithElement (CTE)
SELECT 'WithElement' AS t, formatQueryFromJSON(parseQueryToJSON('WITH x AS (SELECT 1 AS a) SELECT * FROM x'));

-- SetQuery
SELECT 'SetQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SET max_threads = 4'));

-- ExplainQuery variants
SELECT 'ExplainQuery_AST' AS t, formatQueryFromJSON(parseQueryToJSON('EXPLAIN AST SELECT 1'));
SELECT 'ExplainQuery_SYNTAX' AS t, formatQueryFromJSON(parseQueryToJSON('EXPLAIN SYNTAX SELECT 1'));
SELECT 'ExplainQuery_PLAN' AS t, formatQueryFromJSON(parseQueryToJSON('EXPLAIN SELECT 1'));

-- === Table/Join types ===

-- TableExpression, TablesInSelectQuery, TablesInSelectQueryElement
SELECT 'TableExpression' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 FROM t'));

-- TableJoin (INNER, LEFT, RIGHT, CROSS)
SELECT 'TableJoin_INNER' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id'));
SELECT 'TableJoin_LEFT' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 LEFT JOIN t2 USING (id)'));
SELECT 'TableJoin_CROSS' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 CROSS JOIN t2'));

-- ArrayJoin
SELECT 'ArrayJoin' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t ARRAY JOIN arr AS x'));
SELECT 'ArrayJoin_LEFT' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT x FROM t LEFT ARRAY JOIN arr AS x'));

-- OrderByElement (ASC, DESC, NULLS FIRST/LAST)
SELECT 'OrderByElement' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST'));

-- OrderByElement with FILL
SELECT 'OrderByElement_FILL' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a WITH FILL FROM 1 TO 10 STEP 1'));

-- WindowDefinition, WindowListElement
SELECT 'WindowDefinition' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT row_number() OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t'));
SELECT 'WindowListElement' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT row_number() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)'));

-- SampleRatio
SELECT 'SampleRatio' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t SAMPLE 1/10'));
SELECT 'SampleRatio_offset' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t SAMPLE 0.1 OFFSET 0.5'));

-- InterpolateElement
SELECT 'InterpolateElement' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t ORDER BY a WITH FILL INTERPOLATE (b AS b + 1)'));

-- Collation
SELECT 'Collation' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t ORDER BY a COLLATE ''en'''));

-- === DDL types ===

-- CreateQuery, ColumnDeclaration, DataType, Storage, Columns definition
SELECT 'CreateQuery' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE db.t (id UInt64, name String DEFAULT ''x'', ts DateTime) ENGINE = MergeTree ORDER BY id'));

-- CreateQuery with INDEX (IndexDeclaration)
SELECT 'IndexDeclaration' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a String, INDEX idx a TYPE bloom_filter GRANULARITY 4) ENGINE = MergeTree ORDER BY a'));

-- CreateQuery with CONSTRAINT (ConstraintDeclaration)
SELECT 'ConstraintDeclaration' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'));

-- CreateQuery with PROJECTION (ProjectionDeclaration)
SELECT 'ProjectionDeclaration' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b UInt64, PROJECTION p (SELECT a, sum(b) GROUP BY a)) ENGINE = MergeTree ORDER BY a'));

-- CreateQuery with TTL (TTLElement)
SELECT 'TTLElement' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, d DateTime) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY'));

-- CreateQuery with PARTITION BY (Partition)
SELECT 'Partition' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, d Date) ENGINE = MergeTree PARTITION BY d ORDER BY a'));

-- CreateQuery AS SELECT
SELECT 'CreateQuery_AS_SELECT' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a'));

-- CreateQuery VIEW
SELECT 'CreateView' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v AS SELECT 1'));

-- DropQuery (TABLE, DATABASE, VIEW)
SELECT 'DropQuery_TABLE' AS t, formatQueryFromJSON(parseQueryToJSON('DROP TABLE IF EXISTS t'));
SELECT 'DropQuery_DATABASE' AS t, formatQueryFromJSON(parseQueryToJSON('DROP DATABASE IF EXISTS db'));

-- InsertQuery
SELECT 'InsertQuery_SELECT' AS t, formatQueryFromJSON(parseQueryToJSON('INSERT INTO t SELECT 1, 2'));
SELECT 'InsertQuery_COLUMNS' AS t, formatQueryFromJSON(parseQueryToJSON('INSERT INTO t (a, b) SELECT 1, 2'));

-- AlterQuery, AlterCommand (ADD, DROP, MODIFY, RENAME COLUMN)
SELECT 'AlterQuery_ADD' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ADD COLUMN c UInt64'));
SELECT 'AlterQuery_DROP' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP COLUMN c'));
SELECT 'AlterQuery_MODIFY' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY COLUMN c String'));
SELECT 'AlterQuery_RENAME' AS t, formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t RENAME COLUMN a TO b'));

-- RenameQuery
SELECT 'RenameQuery' AS t, formatQueryFromJSON(parseQueryToJSON('RENAME TABLE a TO b'));
SELECT 'RenameQuery_DB' AS t, formatQueryFromJSON(parseQueryToJSON('RENAME TABLE db1.a TO db2.b'));

-- NameTypePair (used in CAST)
SELECT 'NameTypePair' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT CAST(1, ''UInt64'')'));

-- FunctionWithKeyValueArguments, Pair (used in engines/settings)
SELECT 'FunctionWithKV' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192'));

-- === Dictionary types ===

-- Dictionary, DictionaryLifetime, DictionaryLayout, DictionaryRange, DictionaryAttributeDeclaration
SELECT 'Dictionary' AS t, formatQueryFromJSON(parseQueryToJSON('CREATE DICTIONARY dict (id UInt64, name String DEFAULT '''') PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE ''t'')) LAYOUT(HASHED()) LIFETIME(MIN 0 MAX 1000)'));

-- === System/Show/Control types ===

-- SystemQuery
SELECT 'SystemQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SYSTEM FLUSH LOGS'));
SELECT 'SystemQuery_STOP' AS t, formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP MERGES t'));

-- ShowTablesQuery
SELECT 'ShowTablesQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW TABLES FROM db'));
SELECT 'ShowTablesQuery_LIKE' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW TABLES LIKE ''%test%'''));
SELECT 'ShowDatabases' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW DATABASES'));

-- ShowColumnsQuery
SELECT 'ShowColumnsQuery' AS t, formatQueryFromJSON(parseQueryToJSON('SHOW COLUMNS FROM t'));

-- KillQueryQuery
SELECT 'KillQueryQuery' AS t, formatQueryFromJSON(parseQueryToJSON('KILL QUERY WHERE query_id = ''abc'''));

-- OptimizeQuery
SELECT 'OptimizeQuery' AS t, formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL'));
SELECT 'OptimizeQuery_DEDUP' AS t, formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t FINAL DEDUPLICATE'));

-- UseQuery
SELECT 'UseQuery' AS t, formatQueryFromJSON(parseQueryToJSON('USE mydb'));

-- CheckTableQuery
SELECT 'CheckTableQuery' AS t, formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t'));

-- DeleteQuery (lightweight delete)
SELECT 'DeleteQuery' AS t, formatQueryFromJSON(parseQueryToJSON('DELETE FROM t WHERE id = 1'));

-- === Misc types ===

-- Alias handling
SELECT 'Alias' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a AS x, b AS y FROM t AS u'));

-- Lambda function
SELECT 'Lambda' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT arrayMap(x -> x * 2, [1, 2, 3])'));

-- GLOBAL JOIN
SELECT 'GlobalJoin' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 GLOBAL LEFT JOIN t2 ON t1.id = t2.id'));

-- Table function
SELECT 'TableFunction' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT * FROM numbers(10)'));

-- FINAL
SELECT 'Final' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t FINAL'));

-- GROUP BY with modifiers
SELECT 'GroupBy_ROLLUP' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY ROLLUP(a, b)'));
SELECT 'GroupBy_CUBE' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b, count() FROM t GROUP BY CUBE(a, b)'));

-- LIMIT BY
SELECT 'LimitBy' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM t LIMIT 1 BY a'));

-- QUALIFY
SELECT 'Qualify' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a, row_number() OVER (ORDER BY a) AS rn FROM t QUALIFY rn <= 3'));

-- SETTINGS in SELECT
SELECT 'SelectSettings' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 SETTINGS max_threads = 1'));

-- Multiple JOINs
SELECT 'MultiJoin' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT a FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id'));

-- UNION DISTINCT
SELECT 'UnionDistinct' AS t, formatQueryFromJSON(parseQueryToJSON('SELECT 1 UNION DISTINCT SELECT 1'));

-- === Idempotence test: double round-trip ===
SELECT 'Idempotent' AS t, formatQueryFromJSON(parseQueryToJSON(formatQueryFromJSON(parseQueryToJSON('SELECT a, sum(b) FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE c > 0 GROUP BY a ORDER BY a LIMIT 10')))) = formatQueryFromJSON(parseQueryToJSON('SELECT a, sum(b) FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE c > 0 GROUP BY a ORDER BY a LIMIT 10'));
