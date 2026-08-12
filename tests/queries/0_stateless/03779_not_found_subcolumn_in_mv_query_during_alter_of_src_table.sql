SET enable_analyzer=1;


DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP VIEW IF EXISTS mv;

CREATE TABLE src
(
    data Array(Tuple(id UInt32)),
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
    data_joined.id AS id
FROM
(
    SELECT arrayJoin(data) as data_joined
    FROM src
)
WHERE data_joined.id != 42;

INSERT INTO src VALUES ([tuple(1), tuple(2)], 0), ([], 1), ([tuple(42)], 2);

ALTER TABLE src DROP COLUMN dummy;

SELECT * FROM src;
SELECT * FROM dst;

DROP VIEW mv;
DROP TABLE dst;
DROP TABLE src;
