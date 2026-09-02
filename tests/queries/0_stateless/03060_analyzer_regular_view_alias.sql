-- https://github.com/ClickHouse/ClickHouse/issues/11068
SET enable_analyzer=1;
create table vt(datetime_value DateTime, value Float64) Engine=Memory;

create view computed_datum_hours as
SELECT
        toStartOfHour(b.datetime_value) AS datetime_desc,
        sum(b.value) AS value
FROM vt AS b
GROUP BY toStartOfHour(b.datetime_value);

SELECT
    toStartOfHour(b.datetime_value) AS datetime_desc,
    sum(b.value) AS value
FROM vt AS b
GROUP BY toStartOfHour(b.datetime_value);
