DROP TABLE IF EXISTS test.cast_enums;
CREATE TABLE test.cast_enums
(
    type Enum8('session' = 1, 'pageview' = 2, 'click' = 3),
    date Date,
    id UInt64
) ENGINE = MergeTree(date, (type, date, id), 8192);

INSERT INTO test.cast_enums SELECT 'session' AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;
INSERT INTO test.cast_enums SELECT 2 AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;

SELECT type, date, id FROM test.cast_enums ORDER BY type, id;

DROP TABLE IF EXISTS test.cast_enums;
