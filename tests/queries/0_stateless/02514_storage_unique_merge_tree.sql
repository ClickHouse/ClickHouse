DROP TABLE IF EXISTS test_unique_mergetree;

CREATE TABLE test_unique_mergetree(
	n1 UInt32,
	n2 UInt32,
	s String
) ENGINE= UniqueMergeTree ORDER BY n1;

INSERT INTO test_unique_mergetree SELECT number, 1, 'hello' FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1;

INSERT INTO test_unique_mergetree SELECT number, 2, 'world' FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1;

INSERT INTO test_unique_mergetree SELECT number, 3, 'hello, world' FROM numbers(5, 5);

SELECT * FROM test_unique_mergetree ORDER BY n1, n2;

DROP TABLE test_unique_mergetree;

CREATE TABLE test_unique_mergetree(
	n1 UInt32,
	n2 UInt32,
	s String,
	v UInt32
) ENGINE=UniqueMergeTree(v) ORDER BY n1;

INSERT INTO test_unique_mergetree SELECT number, number+1, 'a', number % 2 FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1, n2, s, v;

INSERT INTO test_unique_mergetree SELECT number, number+1, 'b', number FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1, n2, s, v;

DROP TABLE test_unique_mergetree;

CREATE TABLE test_unique_mergetree(
	n1 UInt32,
	n2 UInt32,
	s String,
	v UInt32
) ENGINE=UniqueMergeTree(v) ORDER BY (n1, n2) UNIQUE KEY n1;

INSERT INTO test_unique_mergetree SELECT number, number+1, 'a', number % 2 FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1, n2, s, v;

INSERT INTO test_unique_mergetree SELECT number, number+1, 'b', number FROM numbers(10);

SELECT * FROM test_unique_mergetree ORDER BY n1, n2, s, v;

INSERT INTO test_unique_mergetree SELECT number, number+1, 'c', number FROM numbers(20, 30);
INSERT INTO test_unique_mergetree SELECT number, number+1, 'd', number FROM numbers(30, 40);
INSERT INTO test_unique_mergetree SELECT number, number+1, 'e', number FROM numbers(40, 50);
INSERT INTO test_unique_mergetree SELECT number, number+1, 'f', number FROM numbers(50, 60);
INSERT INTO test_unique_mergetree SELECT number, number+1, 'g', number FROM numbers(60, 70);

SELECT s, sum(n1), count(n2) from test_unique_mergetree group by s order by s;

SELECT n1, n2 from test_unique_mergetree where s < 'f' order by n1, n2;
SELECT n1, n2, v from test_unique_mergetree where s > 'c' and v < 100 order by n1, n2, v;

DROP TABLE test_unique_mergetree;
