DROP TABLE IF EXISTS events;
CREATE TABLE events
(
    `Id` UInt64,
    `Payload` String,
    `Time` DateTime
)
ENGINE = Memory
;

-- Two blocks in left table
INSERT INTO events SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(500);
INSERT INTO events SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(500, 500);


-- Larger scale
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

DROP TABLE IF EXISTS attributes;
CREATE TABLE attributes
(
    `EventId` UInt64,
    `OtherId` UInt64,
    `Attribute` String
)
ENGINE = MergeTree
ORDER BY EventId;

INSERT INTO attributes SELECT
    sipHash64(number, 1) % 10_000_000 AS EventId,
    sipHash64(number, 2) % 10_000_000 AS OtherId,
    concat('Attribute_', toString(number)) AS Attribute
FROM numbers(1_000_000);

SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SELECT count(), countIf(t1.Attribute != '') AS cnt
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id
SETTINGS log_comment = '03712_nested_loop_join_merge_tree_indexed';

SELECT count(), countIf(t1.Attribute != '') AS cnt
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.OtherId = t0.Id
SETTINGS log_comment = '03712_nested_loop_join_merge_tree_full_scan';

SELECT count(), countIf(t1.Attribute != '') AS cnt
FROM events AS t0 LEFT JOIN attributes AS t1 ON t1.EventId = t0.Id
SETTINGS log_comment = '03712_nested_loop_join_merge_tree_indexed';

SELECT count(), countIf(t1.Attribute != '') AS cnt
FROM events AS t0 LEFT JOIN attributes AS t1 ON t1.OtherId = t0.Id
SETTINGS log_comment = '03712_nested_loop_join_merge_tree_full_scan';

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum , count() AS cnt
FROM events2 AS t0
JOIN attributes AS t1 ON t1.EventId = t0.Id
;

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum , count() AS cnt
FROM events2 AS t0
SEMI LEFT JOIN attributes AS t1 ON t1.EventId = t0.Id
;

SELECT sum(sipHash64(t0.Id, t0.Payload)) AS hash_sum, count() AS cnt
FROM events2 AS t0
ANTI LEFT JOIN attributes AS t1 ON t1.EventId = t0.Id
;

SYSTEM FLUSH LOGS system.query_log;

SELECT
    -- Indexed lookup, right table is not fully scanned
    if(read_rows < 1_000_000, 'OK', format('Fail: {} rows read, query_id={}', read_rows, query_id)),
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 10 AND 1_000_000, 'OK',
        format('Fail: JoinBuildTableRowCount={}, query_id={}', ProfileEvents['JoinBuildTableRowCount'], query_id)),
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_time >= yesterday() AND query_kind = 'Select'
    AND log_comment == '03712_nested_loop_join_merge_tree_indexed'
;

SELECT
    -- Full scan on each lookup
    if(read_rows > 2_000_000, 'OK', format('Fail: {} rows read, query_id="{}"', read_rows, query_id)),
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1_100_000 AND 2_100_000, 'OK',
        format('Fail: JoinBuildTableRowCount={}, query_id="{}"', ProfileEvents['JoinBuildTableRowCount'], query_id)),
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_time >= yesterday() AND query_kind = 'Select'
    AND log_comment == '03712_nested_loop_join_merge_tree_full_scan'
;
