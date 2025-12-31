SET enable_analyzer=1;


DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP VIEW IF EXISTS mv;

CREATE TABLE src
(
    data JSON,
    dummy UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE dst
(
    id String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW mv
TO dst
AS
SELECT
    data.id AS id
FROM
(
    SELECT data
    FROM src
)
WHERE data.id IS NOT NULL;

INSERT INTO src VALUES ('{"id" : 1}', 0), ('{}', 1), ('{"id" : 3}', 2);

ALTER TABLE src DROP COLUMN dummy;

SELECT * FROM src;
SELECT * FROM dst;

DROP VIEW mv;
DROP TABLE dst;
DROP TABLE src;
