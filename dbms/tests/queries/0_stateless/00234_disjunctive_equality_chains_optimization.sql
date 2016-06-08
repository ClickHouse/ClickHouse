CREATE TABLE IF NOT EXISTS test.foo(id UInt64) Engine=Memory;
INSERT INTO test.foo(id) VALUES (0),(4),(1),(1),(3),(1),(1),(2),(2),(2),(1),(2),(3),(2),(1),(1),(2),(1),(1),(1),(3),(1),(2),(2),(1),(1),(3),(1),(2),(1),(1),(3),(2),(1),(1),(4),(0);
SELECT sum(id = 3 OR id = 1 OR id = 2) AS x, sum(id = 3 OR id = 1 OR id = 2) AS x FROM test.foo;
DROP TABLE test.foo;
