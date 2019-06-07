DROP TABLE IF EXISTS test.buffer_00126;
DROP TABLE IF EXISTS test.null_sink_00126;

CREATE TABLE test.null_sink_00126 (a UInt8, b String, c Array(UInt32)) ENGINE = Null;
CREATE TABLE test.buffer_00126 (a UInt8, b String, c Array(UInt32)) ENGINE = Buffer(test, null_sink_00126, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

INSERT INTO test.buffer_00126 VALUES (1, '2', [3]);

SELECT a, b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c FROM test.buffer_00126 ORDER BY a, b, c;

INSERT INTO test.buffer_00126 (c, b, a) VALUES ([7], '8', 9);

SELECT a, b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c FROM test.buffer_00126 ORDER BY a, b, c;

INSERT INTO test.buffer_00126 (a, c) VALUES (11, [33]);

SELECT a, b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT a FROM test.buffer_00126 ORDER BY a, b, c;
SELECT b FROM test.buffer_00126 ORDER BY a, b, c;
SELECT c FROM test.buffer_00126 ORDER BY a, b, c;

DROP TABLE test.buffer_00126;
DROP TABLE test.null_sink_00126;
