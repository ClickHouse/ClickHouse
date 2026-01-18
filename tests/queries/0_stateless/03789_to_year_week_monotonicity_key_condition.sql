DROP TABLE IF EXISTS t;
CREATE TABLE t (s String)
ENGINE = MergeTree
ORDER BY s;

INSERT INTO t VALUES
    ('2020-01-10 00:00:00'),
    ('2020-01-2 00:00:00');

-- This used to return 0 but should be 1
SET use_primary_key = 1;
SELECT count() FROM t
WHERE toYearWeek(s) = toYearWeek('2020-01-2 00:00:00');

SET use_primary_key = 0;
SELECT count() FROM t
WHERE toYearWeek(s) = toYearWeek('2020-01-2 00:00:00');

DROP TABLE IF EXISTS t;
CREATE TABLE t (d Date32)
ENGINE = MergeTree
ORDER BY d;

INSERT INTO t VALUES ('2020-12-31'), ('2021-01-01');

-- This used to return 0 but should be 1
SET use_primary_key = 1;
SELECT count() FROM t
WHERE toWeek(d) = toWeek(toDate32('2020-12-31'));

SET use_primary_key = 0;
SELECT count() FROM t
WHERE toWeek(d) = toWeek(toDate32('2020-12-31'));


DROP TABLE IF EXISTS t;
CREATE TABLE t (s String)
ENGINE = MergeTree
ORDER BY s;

INSERT INTO t VALUES
    ('2020-02-11 00:00:00'),
    ('2020-02-3 00:00:00');

-- This used to throw an error but should return 1
SET use_primary_key = 1;
SELECT count() FROM t
WHERE toWeek(s) = toWeek('2020-02-3 00:00:00');

SET use_primary_key = 0;
SELECT count() FROM t
WHERE toWeek(s) = toWeek('2020-02-3 00:00:00');
