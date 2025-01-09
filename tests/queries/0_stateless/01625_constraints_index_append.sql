-- Tags: no-parallel

-- CNF optimization uses QueryNodeHash to order conditions. We need fixed database.table.column identifier name to stabilize result
DROP DATABASE IF EXISTS db_memory_01625;
CREATE DATABASE db_memory_01625 ENGINE = Memory;
USE db_memory_01625;

DROP TABLE IF EXISTS index_append_test_test;

CREATE TABLE index_append_test_test (i Int64, a UInt32, b UInt64, CONSTRAINT c1 ASSUME i <= 2 * b AND i + 40 > a) ENGINE = MergeTree() ORDER BY i;

INSERT INTO index_append_test_test VALUES (1, 10, 1), (2, 20, 2);

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;

SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT i FROM index_append_test_test WHERE a = 0) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT i FROM index_append_test_test WHERE a < 0) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT i FROM index_append_test_test WHERE a >= 0) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8', '') FROM (EXPLAIN actions=1 SELECT i FROM index_append_test_test WHERE 2 * b < 100) WHERE explain LIKE '%Prewhere%' OR explain LIKE '%Filter column%';

DROP TABLE index_append_test_test;
DROP DATABASE db_memory_01625;
