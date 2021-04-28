SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;

DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.test;

CREATE DATABASE constraint_test;

CREATE TABLE constraint_test.test (i Int64, a UInt32, b UInt64, CONSTRAINT c1 ASSUME i <= 2 * b AND i + 40 > a) ENGINE = MergeTree() ORDER BY i;
INSERT INTO constraint_test.test VALUES (1, 10, 1), (2, 20, 2);

EXPLAIN SYNTAX SELECT i FROM constraint_test.test WHERE a = 0;
EXPLAIN SYNTAX SELECT i FROM constraint_test.test WHERE a < 0;
EXPLAIN SYNTAX SELECT i FROM constraint_test.test WHERE a >= 0;
EXPLAIN SYNTAX SELECT i FROM constraint_test.test WHERE 2 * b < 100;

DROP TABLE constraint_test.test;

DROP DATABASE constraint_test;