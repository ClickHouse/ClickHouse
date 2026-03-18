SET join_algorithm = 'full_sorting_merge';
SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS events0;

CREATE TABLE events0 (
    begin Float64,
    value Int32
) ENGINE = MergeTree ORDER BY begin;

INSERT INTO events0 VALUES (1.0, 0), (3.0, 1), (6.0, 2), (8.0, 3);

SELECT p.ts, e.value
FROM
    (SELECT number :: Float64 AS ts FROM numbers(10)) p
ASOF  JOIN events0 e
ON p.ts >= e.begin
ORDER BY p.ts ASC;

SELECT p.ts, e.value
FROM
    (SELECT number :: Float64 AS ts FROM numbers(10)) p
ASOF LEFT JOIN events0 e
ON p.ts >= e.begin
ORDER BY p.ts ASC
-- SETTINGS join_use_nulls = 1
;

DROP TABLE IF EXISTS events0;

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS probes;

CREATE TABLE events (
    key Int32,
    begin Float64,
    value Int32
) ENGINE = MergeTree ORDER BY (key, begin);

INSERT INTO events VALUES (1, 1.0, 0), (1, 3.0, 1), (1, 6.0, 2), (1, 8.0, 3), (2, 0.0, 10), (2, 7.0, 20), (2, 11.0, 30);

CREATE TABLE probes (
    key Int32,
    ts Float64
) ENGINE = MergeTree ORDER BY (key, ts) AS
SELECT
    key.number,
    ts.number
FROM
    numbers(1, 2) as key,
    numbers(10) as ts
SETTINGS join_algorithm = 'hash';

SELECT p.key, p.ts, e.value
FROM probes p
ASOF JOIN events e
ON p.key = e.key AND p.ts >= e.begin
ORDER BY p.key, p.ts ASC;

SELECT p.key, p.ts, e.value
FROM probes p
ASOF LEFT JOIN events e
ON p.key = e.key AND p.ts >= e.begin
ORDER BY p.key, p.ts ASC NULLS FIRST;

