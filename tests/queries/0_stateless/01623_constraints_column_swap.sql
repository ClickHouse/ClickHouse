SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;

DROP DATABASE IF EXISTS constraint_test;
DROP TABLE IF EXISTS constraint_test.test;

CREATE DATABASE constraint_test;

CREATE TABLE constraint_test.test (i Int64, a String, b UInt64, CONSTRAINT c1 ASSUME b = cityHash64(a)) ENGINE = MergeTree() ORDER BY i;
INSERT INTO constraint_test.test VALUES (1, 'cat', 1), (2, 'dog', 2);

EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM constraint_test.test WHERE cityHash64(a) = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM constraint_test.test WHERE cityHash64(a) = 0;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM constraint_test.test WHERE b = 0;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM constraint_test.test WHERE b = 1;

DROP TABLE constraint_test.test;


CREATE TABLE constraint_test.test (i Int64, a String, b String, CONSTRAINT c1 ASSUME a = substring(reverse(b), 1, 1)) ENGINE = MergeTree() ORDER BY i;
INSERT INTO constraint_test.test VALUES (1, 'c', 'aaaaaaaaaaaaaaaaaac'), (2, 'd', 'bbbbbbbbbbbbbbbd');

EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM constraint_test.test WHERE a = 'c';
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM constraint_test.test WHERE substring(reverse(b), 1, 1) = 'c';

DROP TABLE constraint_test.test;

DROP DATABASE constraint_test;
