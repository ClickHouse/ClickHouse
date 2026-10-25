-- https://github.com/ClickHouse/ClickHouse/issues/65981
SET allow_experimental_analyzer = 1;
DROP TABLE IF EXISTS input;
DROP TABLE IF EXISTS deduplicate;
DROP TABLE IF EXISTS deduplicate_mv;
DROP TABLE IF EXISTS event;

CREATE TABLE input (json_message String) ENGINE = MergeTree ORDER BY json_message;

CREATE TABLE deduplicate
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY (id);

CREATE TABLE event
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY (id);

CREATE MATERIALIZED VIEW deduplicate_mv TO deduplicate
AS 
WITH event AS
    (
        SELECT
            JSONExtract(json_message, 'id', 'Nullable(UInt64)') AS id
        FROM input
        WHERE (id IS NOT NULL)
    )
SELECT DISTINCT *
FROM event
WHERE id NOT IN
(
    SELECT id
    FROM deduplicate
    WHERE id IN
    (
        SELECT id
        FROM event
    )
);

INSERT INTO input VALUES ('{"id":5}');

SELECT * FROM deduplicate_mv FORMAT Null;
