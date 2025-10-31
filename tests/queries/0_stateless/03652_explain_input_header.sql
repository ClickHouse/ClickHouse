SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (x Int32, y String) ENGINE = Memory;
CREATE TABLE t2 (x Int32, y String) ENGINE = Memory;

INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- All possible combinations of header and input_header options
EXPLAIN PLAN header = 0, input_headers = 0 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN header = 0, input_headers = 1 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN header = 1, input_headers = 0 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN header = 1, input_headers = 1 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;

-- One complex query with multiple input headers
EXPLAIN PLAN header = 1, input_headers = 1
(
    SELECT * FROM t1 INNER JOIN t2 USING x WHERE t1.x > 2 OR t2.y = 'a' LIMIT 10
) UNION ALL (
    SELECT * FROM t2 LEFT JOIN t1 USING x WHERE t2.x < 2 OR t1.y = 'c' LIMIT 10
);

-- Same as above, but in JSON format
-- All possible combinations of header and input_header options
EXPLAIN PLAN json = 1, header = 0, input_headers = 0 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN json = 1, header = 0, input_headers = 1 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN json = 1, header = 1, input_headers = 0 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;
EXPLAIN PLAN json = 1, header = 1, input_headers = 1 SELECT * FROM t1 WHERE x > 2 OR y = 'a' LIMIT 10;

-- One complex query with multiple input headers
EXPLAIN PLAN json = 1, header = 1, input_headers = 1
(
    SELECT * FROM t1 INNER JOIN t2 USING x WHERE t1.x > 2 OR t2.y = 'a' LIMIT 10
) UNION ALL (
    SELECT * FROM t2 LEFT JOIN t1 USING x WHERE t2.x < 2 OR t1.y = 'c' LIMIT 10
);
