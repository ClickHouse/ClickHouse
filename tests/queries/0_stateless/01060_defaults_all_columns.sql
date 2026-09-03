DROP TABLE IF EXISTS defaults_all_columns;

CREATE TABLE defaults_all_columns (n UInt8 DEFAULT 42, s String DEFAULT concat('test', CAST(n, 'String'))) ENGINE = Memory;

INSERT INTO defaults_all_columns FORMAT JSONEachRow {"n": 1, "s": "hello"} {};

INSERT INTO defaults_all_columns FORMAT JSONEachRow {"n": 2}, {"s": "world"};

SELECT * FROM defaults_all_columns ORDER BY n, s;

DROP TABLE defaults_all_columns;
