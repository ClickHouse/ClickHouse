-- Tags: long

DROP TABLE IF EXISTS events;

CREATE TABLE events
(
    `Id` Nullable(UInt64),
    `Payload` String,
    `Time` DateTime,
)
ENGINE = MergeTree
ORDER BY Time
;

INSERT INTO events SELECT number % 3 + 2, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(100);
INSERT INTO events SELECT NULL, concat('Payload_NULL', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(10);

DROP TABLE IF EXISTS attributes;
CREATE TABLE attributes
(
    `EventId` UInt64,
    `AnotherId` Nullable(UInt64),
    `Attribute` String
)
ENGINE = MergeTree ORDER BY EventId
;

INSERT INTO attributes SELECT 1 AS EventId, 1 AS AnotherId, concat('A_', toString(number)) AS Attribute FROM numbers(100_000);
INSERT INTO attributes SELECT 2 AS EventId, 2 AS AnotherId, concat('B_', toString(number)) AS Attribute FROM numbers(800_000);
INSERT INTO attributes SELECT 3 AS EventId, 3 AS AnotherId, concat('C_', toString(number)) AS Attribute FROM numbers(300_000);
INSERT INTO attributes SELECT 42 AS EventId, NULL AS AnotherId, concat('O_', toString(number)) AS Attribute FROM numbers(200_000);

-- More keys in left table and different data distribution, more distinct keys
DROP TABLE IF EXISTS events2;
CREATE TABLE events2
(
    `Id` UInt64,
    `Payload` String,
    `Time` DateTime
)
ENGINE = MergeTree
ORDER BY Time
;

INSERT INTO events2 SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(1_000_000);

DROP TABLE IF EXISTS attributes2;
CREATE TABLE attributes2
(
    `EventId` UInt64,
    `OtherId` Nullable(UInt32),
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes2 SELECT
    sipHash64(number, 1) % 10_000_000 AS EventId,
    sipHash64(number, 1) % 10_000_000 AS OtherId,
    concat('Attribute_', toString(number)) AS Attribute
FROM numbers(1_000_000);

INSERT INTO attributes2 SELECT 32 AS EventId, 32 AS OtherId, 'Attribute_Dup' AS Attribute;
INSERT INTO attributes2 SELECT 1_000_001 AS EventId, NULL AS OtherId, 'Attribute_Dup' AS Attribute;

SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SELECT 'INNER';
SELECT t0.Id, sum(sipHash64(t0.Payload)), count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute)) AS attr_hash_sum
FROM events AS t0
JOIN attributes AS t1 ON t0.Id = t1.EventId
GROUP BY t0.Id
ORDER BY t0.Id NULLS FIRST
;

SELECT 'LEFT';
SELECT t0.Id, sum(sipHash64(t0.Payload)), count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute)) AS attr_hash_sum
FROM events AS t0
LEFT JOIN attributes AS t1 ON t0.Id = t1.EventId
GROUP BY t0.Id
ORDER BY t0.Id NULLS FIRST
;

SELECT 'SEMI LEFT';
SELECT t0.Id, sum(sipHash64(t0.Payload)), count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute)) AS attr_hash_sum
FROM events AS t0
SEMI LEFT JOIN attributes AS t1 ON t0.Id = t1.EventId
GROUP BY t0.Id
ORDER BY t0.Id NULLS FIRST
;

SELECT 'ANTI LEFT';
SELECT t0.Id, sum(sipHash64(t0.Payload)), count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute)) AS attr_hash_sum
FROM events AS t0
ANTI LEFT JOIN attributes AS t1 ON t0.Id = t1.EventId
GROUP BY t0.Id
ORDER BY t0.Id NULLS FIRST
;

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum , count() AS cnt
FROM events2 AS t0
JOIN attributes2 AS t1 ON t1.EventId = t0.Id
;

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum , count() AS cnt
FROM events2 AS t0
SEMI LEFT JOIN attributes2 AS t1 ON t1.EventId = t0.Id
;

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum, count() AS cnt
FROM events2 AS t0
ANTI LEFT JOIN attributes2 AS t1 ON t1.EventId = t0.Id
;
