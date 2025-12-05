DROP TABLE IF EXISTS avg_dates;
CREATE TABLE avg_dates
(
    d Date,
    d32 Date32,
    dt DateTime,
    dt64 DateTime64(3)
)
ENGINE = Memory;

INSERT INTO avg_dates VALUES
    ('2024-01-01', '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00.000'),
    ('2024-01-03', '2024-01-03', '2024-01-03 12:00:00', '2024-01-03 12:00:00.000');

SELECT
    avg(d) AS avg_date,
    avg(d32) AS avg_date32,
    avg(dt) AS avg_datetime,
    avg(dt64) AS avg_datetime64
FROM avg_dates;


SELECT
    avg(d) AS avg_date_empty,
    avg(d32) AS avg_date32_empty,
    avg(dt) AS avg_datetime_empty,
    avg(dt64) AS avg_datetime64_empty
FROM avg_dates
WHERE d < '1970-01-01';


DROP TABLE IF EXISTS avg_dates_single;
CREATE TABLE avg_dates_single (d Date, dt DateTime) ENGINE = Memory;

INSERT INTO avg_dates_single VALUES ('2024-06-15', '2024-06-15 14:30:00');

SELECT avg(d) AS avg_date_single, avg(dt) AS avg_datetime_single
FROM avg_dates_single;


DROP TABLE IF EXISTS avg_dates_odd;
CREATE TABLE avg_dates_odd (d Date, dt DateTime) ENGINE = Memory;

INSERT INTO avg_dates_odd VALUES
    ('2024-01-01', '2024-01-01 00:00:00'),
    ('2024-01-02', '2024-01-02 00:00:00'),
    ('2024-01-04', '2024-01-04 00:00:00');

SELECT avg(d) AS avg_date_odd, avg(dt) AS avg_datetime_odd
FROM avg_dates_odd;


DROP TABLE IF EXISTS avg_datetime_tz;
CREATE TABLE avg_datetime_tz (dt DateTime('UTC')) ENGINE = Memory;

INSERT INTO avg_datetime_tz VALUES
    ('2024-01-01 00:00:00'),
    ('2024-01-03 00:00:00');

SELECT avg(dt) AS avg_datetime_tz FROM avg_datetime_tz;


DROP TABLE IF EXISTS avg_dt64_scales;
CREATE TABLE avg_dt64_scales
(
    dt64_0 DateTime64(0),
    dt64_3 DateTime64(3),
    dt64_6 DateTime64(6)
)
ENGINE = Memory;

INSERT INTO avg_dt64_scales VALUES
    ('2024-01-01 00:00:00', '2024-01-01 00:00:00.000', '2024-01-01 00:00:00.000000'),
    ('2024-01-05 00:00:00', '2024-01-05 00:00:00.000', '2024-01-05 00:00:00.000000');

SELECT
    avg(dt64_0) AS avg_dt64_0,
    avg(dt64_3) AS avg_dt64_3,
    avg(dt64_6) AS avg_dt64_6
FROM avg_dt64_scales;


DROP TABLE IF EXISTS avg_date32_range;
CREATE TABLE avg_date32_range (d32 Date32) ENGINE = Memory;

INSERT INTO avg_date32_range VALUES
    ('1900-01-01'),
    ('2299-12-31');

SELECT avg(d32) AS avg_date32_range FROM avg_date32_range;


DROP TABLE IF EXISTS avg_dates_grouped;
CREATE TABLE avg_dates_grouped (category String, d Date, dt DateTime) ENGINE = Memory;

INSERT INTO avg_dates_grouped VALUES
    ('A', '2024-01-01', '2024-01-01 00:00:00'),
    ('A', '2024-01-03', '2024-01-03 00:00:00'),
    ('B', '2024-02-01', '2024-02-01 00:00:00'),
    ('B', '2024-02-05', '2024-02-05 00:00:00');

SELECT
    category,
    avg(d) AS avg_date_grouped,
    avg(dt) AS avg_datetime_grouped
FROM avg_dates_grouped
GROUP BY category
ORDER BY category;


DROP TABLE IF EXISTS avg_dates_empty_table;
CREATE TABLE avg_dates_empty_table (d Date, dt DateTime) ENGINE = Memory;

SELECT avg(d) AS avg_date_empty_table, avg(dt) AS avg_datetime_empty_table
FROM avg_dates_empty_table;

-- Cleanup
DROP TABLE IF EXISTS avg_dates;
DROP TABLE IF EXISTS avg_dates_single;
DROP TABLE IF EXISTS avg_dates_odd;
DROP TABLE IF EXISTS avg_datetime_tz;
DROP TABLE IF EXISTS avg_dt64_scales;
DROP TABLE IF EXISTS avg_date32_range;
DROP TABLE IF EXISTS avg_dates_grouped;
DROP TABLE IF EXISTS avg_dates_empty_table;