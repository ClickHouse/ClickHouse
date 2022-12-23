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

DROP TABLE test_unique_mergetree;
