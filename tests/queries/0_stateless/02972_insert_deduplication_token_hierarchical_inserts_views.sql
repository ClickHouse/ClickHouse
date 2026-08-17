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

DROP VIEW IF EXISTS mv_1_2;
CREATE MATERIALIZED VIEW mv_1_2 TO ds_1_1 as
SELECT
    timestamp t, sum(value) v
FROM landing
GROUP BY t;

INSERT INTO landing SELECT 1 as timestamp, 1 AS value FROM numbers(10);

SELECT sleep(3);

INSERT INTO landing SELECT 1 as timestamp, 1 AS value FROM numbers(10);

SYSTEM FLUSH LOGS part_log;
SELECT table, name, error FROM system.part_log
WHERE database = currentDatabase()
ORDER BY table, name;

SELECT count() FROM landing;

DROP TABLE landing;

DROP TABLE ds_1_1;
DROP VIEW mv_1_1;
DROP VIEW mv_1_2;
