-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101727
-- Tuples of different sizes in VALUES should not cause OOB access in template parser.
DROP TABLE IF EXISTS t_tuple_size_mismatch;
CREATE TABLE t_tuple_size_mismatch (c0 String) ENGINE = Memory;

INSERT INTO TABLE t_tuple_size_mismatch (c0) VALUES ((1, 2)), ((1, 2, 3));
INSERT INTO TABLE t_tuple_size_mismatch (c0) VALUES ((1, 2, 3)), ((1, 2));

SELECT * FROM t_tuple_size_mismatch ORDER BY c0;
DROP TABLE t_tuple_size_mismatch;
