DROP TABLE IF EXISTS partitioned_by_tuple;

CREATE TABLE partitioned_by_tuple (d Date, x UInt8, w String, y UInt8) ENGINE SummingMergeTree (y) PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO partitioned_by_tuple VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO partitioned_by_tuple VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO partitioned_by_tuple VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE TABLE partitioned_by_tuple;

SELECT * FROM partitioned_by_tuple ORDER BY d, x, w, y;

OPTIMIZE TABLE partitioned_by_tuple FINAL;

SELECT * FROM partitioned_by_tuple ORDER BY d, x, w, y;

DROP TABLE partitioned_by_tuple;
