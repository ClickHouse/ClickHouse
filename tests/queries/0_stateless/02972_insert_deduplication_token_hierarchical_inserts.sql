SET insert_deduplicate = 1;
SET deduplicate_blocks_in_dependent_materialized_views = 1;
SET update_insert_deduplication_token_in_dependent_materialized_views = 1;
SET insert_deduplication_token = 'test';

DROP TABLE IF EXISTS landing;
CREATE TABLE landing
(
    timestamp UInt64,
    value UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP TABLE IF EXISTS ds_1_1;
CREATE TABLE ds_1_1
(
    t UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP VIEW IF EXISTS mv_1_1;
CREATE MATERIALIZED VIEW mv_1_1 TO ds_1_1 as
SELECT
    timestamp t, sum(value) v
FROM landing
GROUP BY t;

DROP TABLE IF EXISTS ds_1_2;
CREATE TABLE ds_1_2
(
    t UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP VIEW IF EXISTS mv_1_2;
CREATE MATERIALIZED VIEW mv_1_2 TO ds_1_2 as
SELECT
    timestamp t, sum(value) v
FROM landing
GROUP BY t;

DROP TABLE IF EXISTS ds_2_1;
CREATE TABLE ds_2_1
(
    l String,
    t DateTime,
    v UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP VIEW IF EXISTS mv_2_1;
CREATE MATERIALIZED VIEW mv_2_1 TO ds_2_1 as
SELECT '2_1' l, t, v
FROM ds_1_1;

DROP VIEW IF EXISTS mv_2_2;
CREATE MATERIALIZED VIEW mv_2_2 TO ds_2_1 as
SELECT '2_2' l, t, v
FROM ds_1_2;

DROP TABLE IF EXISTS ds_3_1;
CREATE TABLE ds_3_1
(
    l String,
    t DateTime,
    v UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000;

DROP VIEW IF EXISTS mv_3_1;
CREATE MATERIALIZED VIEW mv_3_1 TO ds_3_1 as
SELECT '3_1' l, t, v
FROM ds_2_1;

INSERT INTO landing SELECT 1 as timestamp, 1 AS value FROM numbers(10);

SELECT sleep(3);

INSERT INTO landing SELECT 1 as timestamp, 1 AS value FROM numbers(10);

SYSTEM FLUSH LOGS;
SELECT table, name, error FROM system.part_log
WHERE database = currentDatabase()
ORDER BY table, name;

SELECT count() FROM landing;

DROP TABLE landing;

DROP TABLE ds_1_1;
DROP VIEW mv_1_1;

DROP TABLE ds_1_2;
DROP VIEW mv_1_2;

DROP TABLE ds_2_1;
DROP VIEW mv_2_1;
DROP VIEW mv_2_2;

DROP TABLE ds_3_1;
DROP VIEW mv_3_1;
