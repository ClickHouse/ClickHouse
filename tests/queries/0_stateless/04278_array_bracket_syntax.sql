-- PostgreSQL-compatible ARRAY[...] syntax sugar: equivalent to [...].

-- Basic types
SELECT ARRAY[1, 2, 3];
SELECT ARRAY[1.5, 2.5];
SELECT ARRAY['a', 'b', 'c'];
SELECT ARRAY[true, false];

-- Empty array
SELECT ARRAY[];

-- Expressions inside
SELECT ARRAY[1 + 1, 2 * 3, 10 - 4];

-- Nested arrays
SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]];

-- Mixed with regular bracket syntax
SELECT ARRAY[1, 2] = [1, 2];

-- Chained subscript: array constructor result can be directly subscripted
SELECT ARRAY[1, 2, 3][2];

-- ARRAY as identifier should still work
SELECT [1, 2, 3] AS ARRAY;

-- INSERT with ARRAY[...] (primary motivation from the issue)
CREATE TABLE test_array_insert (v Array(UInt32)) ENGINE = Memory;
INSERT INTO test_array_insert VALUES (ARRAY[10000, 10000, 10000]);
SELECT v FROM test_array_insert;
DROP TABLE test_array_insert;

-- Dictionary attribute DEFAULT uses ParserArrayOfLiterals, not the main expression parser
CREATE DICTIONARY test_array_dict (id UInt64, arr Array(UInt64) DEFAULT ARRAY[1, 2, 3])
PRIMARY KEY id SOURCE(NULL()) LIFETIME(0) LAYOUT(FLAT());
DROP DICTIONARY test_array_dict;

-- Nested ARRAY[ARRAY[...]] in dictionary DEFAULT (tests parseAllCollectionsStart)
CREATE DICTIONARY test_array_nested_dict (id UInt64, arr Array(Array(UInt64)) DEFAULT ARRAY[ARRAY[1, 2], ARRAY[3]])
PRIMARY KEY id SOURCE(NULL()) LIFETIME(0) LAYOUT(FLAT());
DROP DICTIONARY test_array_nested_dict;

-- SET parameter uses ParserAllCollectionsOfLiterals which also goes through parseAllCollectionsStart
SET param_array_test = ARRAY[10, 20, 30];

-- BACKUP SETTINGS uses ParserArray, not the main expression parser.
-- Use formatQuery to verify parsing without executing the backup.
SELECT formatQuery('BACKUP TABLE t TO File(''/tmp/bk/'') SETTINGS cluster_host_ids = ARRAY[ARRAY[''id1'', ''id2'']]') != '';

-- ConstantExpressionTemplate fast path: mixing ARRAY[...] and [...] spellings in the same INSERT
-- must not cause a template-key mismatch. Without the recordLiteralTokens fix, the ARRAY keyword
-- is treated as a fixed token, so [3,4] fails the fast path when ARRAY[1,2] was cached first.
-- Disable the slow expression fallback so the test exercises only the ConstantExpressionTemplate path.
SET input_format_values_interpret_expressions = 0;
CREATE TABLE test_array_values_mixed (arr Array(UInt32)) ENGINE = Memory;
INSERT INTO test_array_values_mixed VALUES (ARRAY[1, 2]), ([3, 4]), (ARRAY[5, 6]);
SELECT arr FROM test_array_values_mixed ORDER BY arr;
DROP TABLE test_array_values_mixed;
SET input_format_values_interpret_expressions = 1;

-- Regression: non-literal ARRAY[...] forms must not share a ConstantExpressionTemplate
-- cache entry with [...]. These forms produce the same AST but differ in fixed tokens
-- (ARRAY keyword vs nothing), so they previously hashed to the same key and caused
-- template-token mismatch failures when mixed in one INSERT.

-- Empty arrays: ARRAY[] and [] share the same AST (array()) and the same result type.
SET input_format_values_interpret_expressions = 0;
CREATE TABLE test_array_empty_mixed (arr Array(UInt32)) ENGINE = Memory;
INSERT INTO test_array_empty_mixed VALUES (ARRAY[]), ([]), (ARRAY[]);
SELECT arr FROM test_array_empty_mixed;
DROP TABLE test_array_empty_mixed;

-- Expression arrays: ARRAY[1+1] and [2+2] share the same AST structure.
CREATE TABLE test_array_expr_mixed (arr Array(UInt32)) ENGINE = Memory;
INSERT INTO test_array_expr_mixed VALUES (ARRAY[1 + 1]), ([2 + 2]), (ARRAY[3 + 3]);
SELECT arr FROM test_array_expr_mixed ORDER BY arr;
DROP TABLE test_array_expr_mixed;

-- All-NULL arrays: ReplaceLiteralsVisitor skips NULL literals, so ARRAY remains a fixed
-- token in the template. ARRAY[NULL] and [NULL] must not share a cache entry.
CREATE TABLE test_array_null_mixed (arr Array(Nullable(UInt32))) ENGINE = Memory;
INSERT INTO test_array_null_mixed VALUES (ARRAY[NULL]), ([NULL]), (ARRAY[NULL]);
SELECT arr FROM test_array_null_mixed;
DROP TABLE test_array_null_mixed;
SET input_format_values_interpret_expressions = 1;

-- Precedence: ARRAY[n] is always an array constructor, never an identifier subscript.
-- This follows PostgreSQL, where ARRAY is a reserved constructor keyword.
-- To subscript a column named ARRAY, quote it: `ARRAY`[n].
SELECT ARRAY[1] FROM (SELECT [10, 20] AS ARRAY);  -- returns [1], not 10
SELECT `ARRAY`[1] FROM (SELECT [10, 20] AS `ARRAY`);  -- returns 10 (quoted identifier)
