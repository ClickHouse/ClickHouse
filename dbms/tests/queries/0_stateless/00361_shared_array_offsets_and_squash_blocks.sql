DROP TABLE IF EXISTS test.nested1;
DROP TABLE IF EXISTS test.nested2;

CREATE TABLE test.nested1 (d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, b String)) ENGINE = MergeTree(d, x, 1);
INSERT INTO test.nested1 (x, n.a, n.b) VALUES (1, ['Hello', 'World'], ['abc', 'def']), (2, [], []);

SET max_block_size = 1;
SELECT * FROM test.nested1 ORDER BY x;

CREATE TABLE test.nested2 (d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, b String)) ENGINE = MergeTree(d, x, 1);

INSERT INTO test.nested2 SELECT * FROM test.nested1;

SELECT * FROM test.nested2 ORDER BY x;

DROP TABLE test.nested1;
DROP TABLE test.nested2;
