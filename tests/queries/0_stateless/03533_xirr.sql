SELECT round(xirr([-10000, 5750, 4250, 3250], [toDate('2020-01-01'), toDate('2020-03-01'), toDate('2020-10-30'), toDate('2021-02-15')]), 6) AS xirr_rate;
SELECT round(xirr([-10000, 5750, 4250, 3250], [toDate32('2020-01-01'), toDate32('2020-03-01'), toDate32('2020-10-30'), toDate32('2021-02-15')]), 6) AS xirr_rate;

SELECT xirr(123, toDate('2020-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xirr([123], toDate('2020-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT xirr(123, [toDate('2020-01-01')]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT round(xirr([-10000], [toDate32('2020-01-01'), toDate32('2020-03-01'), toDate32('2020-10-30'), toDate32('2021-02-15')]), 6) AS xirr_rate; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT round(xirr([-10000, NULL, 4250, 3250], [toDate32('2020-01-01'), toDate32('2020-03-01'), toDate32('2020-10-30'), toDate32('2021-02-15')]), 6) AS xirr_rate; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT xirr([-10000], [toDate('2020-01-01')]);
SELECT xirr([]::Array(Float32), []::Array(Date));
SELECT xirr([-0, 0], [toDate('2020-01-01'), toDate('2020-01-02')]);


CREATE TABLE IF NOT EXISTS 3533_xirr_test (
    tag String,
    date Date,
    date32 Date32,
    value Float64
) ENGINE = Memory;

INSERT INTO 3533_xirr_test VALUES
('a', '2020-01-01', '2020-01-01', -10000),
('a', '2020-06-01', '2020-06-01', 3000),
('a', '2020-12-31', '2020-12-31', 8000),
('b', '2020-03-15', '2020-03-15', -5000),
('b', '2020-09-15', '2020-09-15', 2500),
('b', '2021-03-15', '2021-03-15', 3000),
('c', '2019-12-31', '2019-12-31', -15000),
('c', '2020-04-30', '2020-04-30', 5000),
('c', '2020-08-31', '2020-08-31', 6000),
('c', '2020-12-31', '2020-12-31', 5000),
('c', '2021-02-28', '2021-02-28', 2000),
('d', '2020-01-01', '2020-01-01', -10000),
('d', '2020-03-01', '2020-03-01', 5750),
('d', '2020-10-30', '2020-10-30', 4250),
('d', '2021-02-15', '2021-02-15', 3250)
;

SELECT
    tag,
    round(
        xirr(
            groupArray(value),
            groupArray(date)
        )
    , 6) AS result
FROM (
    SELECT 
        tag,
        date,
        sum(value) as value
    FROM 3533_xirr_test
    GROUP BY tag, date
    ORDER BY tag, date
)
GROUP BY tag
ORDER BY tag;

SELECT 
    tag,
    round(
        xirr(
            groupArray(value),
            groupArray(date32)
        )
    , 6) AS result
FROM (
    SELECT 
        tag,
        date32,
        sum(value) as value
    FROM 3533_xirr_test
    GROUP BY tag, date32
    ORDER BY tag, date32
)
GROUP BY tag
ORDER BY tag;

DROP TABLE IF EXISTS 3533_xirr_test;
