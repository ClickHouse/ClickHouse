SELECT [[[[],[]]]];
SELECT [[1], []];
SELECT [[[[],['']]]];
SELECT concat([], ['Hello'], []);
SELECT arrayPushBack([], 1), arrayPushFront([[]], []);

DROP TABLE IF EXISTS test.arr;
CREATE TABLE test.arr (x Array(String), y Nullable(String), z Array(Array(Nullable(String)))) ENGINE = TinyLog;

INSERT INTO test.arr SELECT [], NULL, [[], [NULL], [NULL, 'Hello']];
SELECT * FROM test.arr;

DROP TABLE test.arr;
