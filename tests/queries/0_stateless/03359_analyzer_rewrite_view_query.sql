-- Tags: no-replicated-database
-- no-replicated-database: avoids strange edge case error that only happens with replicated DB and old analyzer

CREATE TABLE mydestination
(
    `object` String
)
ENGINE = MergeTree
ORDER BY object;

CREATE MATERIALIZED VIEW myview TO mydestination
AS WITH ('foo', 'bar') AS objects
SELECT 'foo' AS object
WHERE object IN (objects);

SELECT * FROM myview;
