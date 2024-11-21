DROP TABLE IF EXISTS events0;
DROP TABLE IF EXISTS probe0;

SET session_timezone = 'UTC';
SET allow_experimental_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET join_use_nulls = 1;

CREATE TABLE events0
ENGINE = MergeTree()
ORDER BY COALESCE(begin, toDateTime('9999-12-31 23:59:59'))
AS
SELECT
    toNullable(toDateTime('2023-03-21 13:00:00') + INTERVAL number HOUR) AS begin,
    number AS value
FROM numbers(4);

INSERT INTO events0 VALUES (NULL, -1), (toDateTime('9999-12-31 23:59:59'), 9);

CREATE TABLE probe0
ENGINE = MergeTree()
ORDER BY COALESCE(begin, toDateTime('9999-12-31 23:59:59'))
AS
SELECT
    toNullable(toDateTime('2023-03-21 12:00:00') + INTERVAL number HOUR) AS begin
FROM numbers(10);

INSERT INTO probe0 VALUES (NULL), (toDateTime('9999-12-31 23:59:59'));

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF JOIN events0 e ON p.begin >= e.begin
ORDER BY p.begin ASC;

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF JOIN events0 e USING (begin)
ORDER BY p.begin ASC
SETTINGS join_use_nulls = 0
;

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF LEFT JOIN events0 e ON p.begin >= e.begin
ORDER BY p.begin ASC;

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF LEFT JOIN events0 e USING (begin)
ORDER BY p.begin ASC
SETTINGS join_use_nulls = 0
;

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF RIGHT JOIN events0 e ON p.begin >= e.begin
ORDER BY e.begin ASC; -- { serverError NOT_IMPLEMENTED}

SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF RIGHT JOIN events0 e USING (begin)
ORDER BY e.begin ASC; -- { serverError NOT_IMPLEMENTED}


SELECT
    p.begin,
    e.value
FROM
    probe0 p
    ASOF LEFT JOIN (
        SELECT * FROM events0 WHERE log(value + 5) > 10
    ) e ON p.begin + INTERVAL 2 HOUR >= e.begin + INTERVAL 1 HOUR
ORDER BY p.begin ASC;


DROP TABLE IF EXISTS events0;
DROP TABLE IF EXISTS probe0;
