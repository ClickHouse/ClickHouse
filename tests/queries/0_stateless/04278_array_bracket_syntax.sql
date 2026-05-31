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

-- BACKUP SETTINGS uses ParserArray, not the main expression parser.
-- Use formatQuery to verify parsing without executing the backup.
SELECT formatQuery('BACKUP TABLE t TO File(''/tmp/bk/'') SETTINGS cluster_host_ids = ARRAY[ARRAY[''id1'', ''id2'']]') != '';
