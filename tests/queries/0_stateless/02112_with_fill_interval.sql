DROP TABLE IF EXISTS with_fill_date;
CREATE TABLE with_fill_date (d Date, d32 Date32) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (toDate('2020-02-05'), toDate32('2020-02-05'));
INSERT INTO with_fill_date VALUES (toDate('2020-02-16'), toDate32('2020-02-16'));
INSERT INTO with_fill_date VALUES (toDate('2020-03-03'), toDate32('2020-03-03'));
INSERT INTO with_fill_date VALUES (toDate('2020-06-10'), toDate32('2020-06-10'));

SELECT '1 DAY';
SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 DAY LIMIT 5;
SELECT '1 WEEK';
SELECT toStartOfWeek(d) as d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 WEEK LIMIT 5;
SELECT '1 MONTH';
SELECT toStartOfMonth(d) as d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 MONTH LIMIT 5;
SELECT '3 MONTH';
SELECT toStartOfMonth(d) as d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL
    FROM toDate('2020-01-01')
    TO toDate('2021-01-01')
    STEP INTERVAL 3 MONTH;

SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 HOUR LIMIT 5; -- { serverError 475 }

SELECT '1 DAY';
SELECT d32, count() FROM with_fill_date GROUP BY d32 ORDER BY d32 WITH FILL STEP INTERVAL 1 DAY LIMIT 5;
SELECT '1 WEEK';
SELECT toStartOfWeek(d32) as d32, count() FROM with_fill_date GROUP BY d32 ORDER BY d32 WITH FILL STEP INTERVAL 1 WEEK LIMIT 5;
SELECT '1 MONTH';
SELECT toStartOfMonth(d32) as d32, count() FROM with_fill_date GROUP BY d32 ORDER BY d32 WITH FILL STEP INTERVAL 1 MONTH LIMIT 5;
SELECT '3 MONTH';
SELECT toStartOfMonth(d32) as d32, count() FROM with_fill_date GROUP BY d32 ORDER BY d32 WITH FILL
    FROM toDate('2020-01-01')
    TO toDate('2021-01-01')
    STEP INTERVAL 3 MONTH;

SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 1 HOUR LIMIT 5; -- { serverError 475 }

DROP TABLE with_fill_date;

DROP TABLE IF EXISTS with_fill_date;
CREATE TABLE with_fill_date (d DateTime('UTC'), d64 DateTime64(3, 'UTC')) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (toDateTime('2020-02-05 10:20:00', 'UTC'), toDateTime64('2020-02-05 10:20:00', 3, 'UTC'));
INSERT INTO with_fill_date VALUES (toDateTime('2020-03-08 11:01:00', 'UTC'), toDateTime64('2020-03-08 11:01:00', 3, 'UTC'));

SELECT '15 MINUTE';
SELECT d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 15 MINUTE LIMIT 5;
SELECT '6 HOUR';
SELECT toStartOfHour(d) as d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 6 HOUR LIMIT 5;
SELECT '10 DAY';
SELECT toStartOfDay(d) as d, count() FROM with_fill_date GROUP BY d ORDER BY d WITH FILL STEP INTERVAL 10 DAY LIMIT 5;

SELECT '15 MINUTE';
SELECT d64, count() FROM with_fill_date GROUP BY d64 ORDER BY d64 WITH FILL STEP INTERVAL 15 MINUTE LIMIT 5;
SELECT '6 HOUR';
SELECT toStartOfHour(d64) as d64, count() FROM with_fill_date GROUP BY d64 ORDER BY d64 WITH FILL STEP INTERVAL 6 HOUR LIMIT 5;
SELECT '10 DAY';
SELECT toStartOfDay(d64) as d64, count() FROM with_fill_date GROUP BY d64 ORDER BY d64 WITH FILL STEP INTERVAL 10 DAY LIMIT 5;

DROP TABLE with_fill_date;

SELECT number FROM numbers(100) ORDER BY number WITH FILL STEP INTERVAL 1 HOUR; -- { serverError 475 }

CREATE TABLE with_fill_date (d Date, id UInt32) ENGINE = Memory;

INSERT INTO with_fill_date VALUES (toDate('2020-02-05'), 1);
INSERT INTO with_fill_date VALUES (toDate('2020-02-16'), 3);
INSERT INTO with_fill_date VALUES (toDate('2020-03-10'), 2);
INSERT INTO with_fill_date VALUES (toDate('2020-03-03'), 3);

SELECT '1 MONTH';

SELECT toStartOfMonth(d) as d, id, count() FROM with_fill_date
GROUP BY d, id
ORDER BY
d WITH FILL
    FROM toDate('2020-01-01')
    TO toDate('2020-05-01')
    STEP INTERVAL 1 MONTH,
id WITH FILL FROM 1 TO 5;

DROP TABLE with_fill_date;
