SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.mergetree;
CREATE TABLE test.mergetree (k UInt32, `n.x` Array(UInt64), `n.y` Array(UInt64)) ENGINE = MergeTree ORDER BY k;

INSERT INTO test.mergetree VALUES (3, [], [1, 2, 3]), (1, [111], []), (2, [], []); -- { serverError 190 }
SELECT * FROM test.mergetree;

INSERT INTO test.mergetree VALUES (3, [4, 5, 6], [1, 2, 3]), (1, [111], [222]), (2, [], []);
SELECT * FROM test.mergetree;

DROP TABLE test.mergetree;
