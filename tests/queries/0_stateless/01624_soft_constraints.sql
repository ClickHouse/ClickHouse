SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;

DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.test;

CREATE DATABASE constraint_test;

CREATE TABLE constraint_test.test (i UInt64, a UInt64, b UInt64, INDEX t (a = b) TYPE hypothesis GRANULARITY 1) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity=1;
INSERT INTO constraint_test.test VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2);

SELECT count() FROM constraint_test.test WHERE a = b;

DROP TABLE constraint_test.test;

DROP DATABASE constraint_test;