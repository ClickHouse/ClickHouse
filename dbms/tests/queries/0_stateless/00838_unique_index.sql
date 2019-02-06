DROP TABLE IF EXISTS test.set_idx;

CREATE TABLE test.set_idx
(
    u64 UInt64,
    i32 Int32,
    f64 Float64,
    d Decimal(10, 2),
    s String,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    dt Date,
    INDEX idx_all (i32, i32 + f64, d, s, e, dt) TYPE set GRANULARITY 1,
    INDEX idx_all2 (i32, i32 + f64, d, s, e, dt) TYPE set GRANULARITY 2,
    INDEX idx_2 (u64 + toYear(dt), substring(s, 2, 4)) TYPE set GRANULARITY 3
) ENGINE = MergeTree()
ORDER BY u64
SETTINGS index_granularity = 2;

INSERT INTO test.set_idx VALUES (0, 5, 4.7, 6.5, 'cba', 'b', '2014-01-04');
INSERT INTO test.set_idx VALUES (1, 5, 4.7, 6.5, 'cba', 'b', '2014-03-11');
INSERT INTO test.set_idx VALUES (2, 2, 4.5, 2.5, 'abc', 'a', '2014-01-01');
INSERT INTO test.set_idx VALUES (3, 5, 6.9, 1.57, 'bac', 'c', '2017-01-01');
INSERT INTO test.set_idx VALUES (4, 2, 4.5, 2.5, 'abc', 'a', '2016-01-01');
INSERT INTO test.set_idx VALUES (5, 5, 6.9, 1.57, 'bac', 'c', '2014-11-11');
INSERT INTO test.set_idx VALUES (6, 2, 4.5, 2.5, 'abc', 'a', '2014-02-11');
INSERT INTO test.set_idx VALUES (7, 5, 6.9, 1.57, 'bac', 'c', '2014-04-11');
INSERT INTO test.set_idx VALUES (8, 2, 4.5, 2.5, 'abc', 'a', '2014-05-11');
INSERT INTO test.set_idx VALUES (9, 5, 6.9, 1.57, 'bac', 'c', '2014-07-11');
INSERT INTO test.set_idx VALUES (12, 5, 4.7, 6.5, 'cba', 'b', '2014-06-11');
INSERT INTO test.set_idx VALUES (13, 5, 4.7, 6.5, 'cba', 'b', '2015-01-01');

OPTIMIZE TABLE test.set_idx;

/* simple select */
SELECT * FROM test.set_idx WHERE i32 = 5 AND i32 + f64 < 12 AND 3 < d AND d < 7 AND (s = 'bac' OR s = 'cba') ORDER BY dt;

/* select with hole made by primary key */
SELECT * FROM test.set_idx WHERE (u64 < 2 OR u64 > 10) AND e != 'b' ORDER BY dt;

DROP TABLE test.set_idx;