DROP TABLE IF EXISTS cast_enums;
CREATE TABLE cast_enums
(
    type Enum8('session' = 1, 'pageview' = 2, 'click' = 3),
    date Date,
    id UInt64
) ENGINE = MergeTree(date, (type, date, id), 8192);

INSERT INTO cast_enums SELECT 'session' AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;
INSERT INTO cast_enums SELECT 2 AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;

SELECT type, date, id FROM cast_enums ORDER BY type, id;

DROP TABLE IF EXISTS cast_enums;
