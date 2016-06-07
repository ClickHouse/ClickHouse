DROP TABLE IF EXISTS test.array_pk;
CREATE TABLE test.array_pk (key Array(UInt8), s String, n UInt64, d Date MATERIALIZED '2000-01-01') ENGINE = MergeTree(d, (key, s, n), 1);

INSERT INTO test.array_pk VALUES ([1, 2, 3], 'Hello, world!', 1);
INSERT INTO test.array_pk VALUES ([1, 2], 'Hello', 2);
INSERT INTO test.array_pk VALUES ([2], 'Goodbye', 3);
INSERT INTO test.array_pk VALUES ([], 'abc', 4);
INSERT INTO test.array_pk VALUES ([2, 3, 4], 'def', 5);
INSERT INTO test.array_pk VALUES ([5, 6], 'ghi', 6);

SELECT * FROM test.array_pk ORDER BY n;

DETACH TABLE test.array_pk;
ATTACH TABLE test.array_pk (key Array(UInt8), s String, n UInt64, d Date MATERIALIZED '2000-01-01') ENGINE = MergeTree(d, (key, s, n), 1);

SELECT * FROM test.array_pk ORDER BY n;

DROP TABLE test.array_pk;
