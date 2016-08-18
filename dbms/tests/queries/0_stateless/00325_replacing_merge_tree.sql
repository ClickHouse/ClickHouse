DROP TABLE IF EXISTS test.replacing;
CREATE TABLE test.replacing (d Date, k UInt64, s String, v UInt16) ENGINE = ReplacingMergeTree(d, k, 8192, v);

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello', 0);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'World', 0);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

OPTIMIZE TABLE test.replacing;
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello', 10);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello!', 9);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'abc', 1);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'def', 1);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'ghi', 0);
SELECT * FROM test.replacing FINAL ORDER BY k, v, _part_index;

OPTIMIZE TABLE test.replacing;
OPTIMIZE TABLE test.replacing;
OPTIMIZE TABLE test.replacing;
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

DROP TABLE test.replacing;


DROP TABLE IF EXISTS test.replacing;
CREATE TABLE test.replacing (d Date, k UInt64, s String, v UInt16) ENGINE = ReplacingMergeTree(d, k, 1, v);

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello', 0);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'World', 0);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

OPTIMIZE TABLE test.replacing;
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello', 10);
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello!', 9);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'abc', 1);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'def', 1);
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'ghi', 0);
SELECT * FROM test.replacing FINAL ORDER BY k, v, _part_index;

OPTIMIZE TABLE test.replacing PARTITION 200001 FINAL;
SELECT _part_index, * FROM test.replacing ORDER BY k, v, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, v, _part_index;

DROP TABLE test.replacing;


DROP TABLE IF EXISTS test.replacing;
CREATE TABLE test.replacing (d Date, k UInt64, s String) ENGINE = ReplacingMergeTree(d, k, 2);

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello');
SELECT _part_index, * FROM test.replacing ORDER BY k, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'World');
SELECT _part_index, * FROM test.replacing ORDER BY k, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, _part_index;

OPTIMIZE TABLE test.replacing;
SELECT _part_index, * FROM test.replacing ORDER BY k, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello');
SELECT _part_index, * FROM test.replacing ORDER BY k, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, _part_index;

INSERT INTO test.replacing VALUES ('2000-01-01', 1, 'Hello!');
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'abc');
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'def');
INSERT INTO test.replacing VALUES ('2000-01-01', 2, 'ghi');
SELECT * FROM test.replacing FINAL ORDER BY k, _part_index;

OPTIMIZE TABLE test.replacing PARTITION 200001 FINAL;
SELECT _part_index, * FROM test.replacing ORDER BY k, _part_index;
SELECT _part_index, * FROM test.replacing FINAL ORDER BY k, _part_index;

DROP TABLE test.replacing;
