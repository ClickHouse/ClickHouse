DROP TABLE IF EXISTS test.partitioned_by_tuple;

CREATE TABLE test.partitioned_by_tuple (d Date, x UInt8, w String, y UInt8) ENGINE SummingMergeTree (y) PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO test.partitioned_by_tuple VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO test.partitioned_by_tuple VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO test.partitioned_by_tuple VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE TABLE test.partitioned_by_tuple;

SELECT * FROM test.partitioned_by_tuple ORDER BY d, x, w, y;

OPTIMIZE TABLE test.partitioned_by_tuple FINAL;

SELECT * FROM test.partitioned_by_tuple ORDER BY d, x, w, y;

DROP TABLE test.partitioned_by_tuple;
