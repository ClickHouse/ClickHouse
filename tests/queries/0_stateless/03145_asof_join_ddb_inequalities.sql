DROP TABLE IF EXISTS events0;
DROP TABLE IF EXISTS probe0;

SET enable_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET date_time_input_format='basic';

CREATE TABLE events0 (
    begin Nullable(DateTime('UTC')),
    value Int32
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO events0 SELECT toDateTime('2023-03-21 13:00:00', 'UTC') + INTERVAL number HOUR, number FROM numbers(4);
INSERT INTO events0 VALUES (NULL, -10),('0000-01-01 00:00:00', -1), ('9999-12-31 23:59:59', 9);

CREATE TABLE probe0 (
    begin Nullable(DateTime('UTC'))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO probe0 SELECT toDateTime('2023-03-21 12:00:00', 'UTC') + INTERVAl number HOUR FROM numbers(10);
INSERT INTO probe0 VALUES (NULL),('9999-12-31 23:59:59');

SET join_use_nulls = 1;

SELECT '-';
SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF JOIN events0 e
ON p.begin > e.begin
ORDER BY p.begin ASC;

SELECT '-';
SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF LEFT JOIN events0 e
ON p.begin > e.begin
ORDER BY p.begin ASC;

SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF JOIN events0 e
ON p.begin <= e.begin
ORDER BY p.begin ASC;

SELECT '-';
SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF LEFT JOIN events0 e
ON p.begin <= e.begin
ORDER BY p.begin ASC;

SELECT '-';
SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF JOIN events0 e
ON p.begin < e.begin
ORDER BY p.begin ASC;

SELECT '-';
SELECT p.begin, e.begin, e.value
FROM probe0 p
ASOF LEFT JOIN events0 e
ON p.begin < e.begin
ORDER BY p.begin ASC;

DROP TABLE IF EXISTS events0;
DROP TABLE IF EXISTS probe0;
