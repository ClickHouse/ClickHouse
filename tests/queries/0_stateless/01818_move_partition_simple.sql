DROP TABLE IF EXISTS main_table_01818;
DROP TABLE IF EXISTS tmp_table_01818;


CREATE TABLE main_table_01818
(
    `id` UInt32,
    `advertiser_id` String,
    `campaign_id` String,
    `name` String,
    `budget` Float64,
    `budget_mode` String,
    `landing_type` String,
    `status` String,
    `modify_time` String,
    `campaign_type` String,
    `campaign_create_time` DateTime,
    `campaign_modify_time` DateTime,
    `create_time` DateTime,
    `update_time` DateTime
)
ENGINE = MergeTree
PARTITION BY advertiser_id
ORDER BY campaign_id
SETTINGS index_granularity = 8192;

CREATE TABLE tmp_table_01818
(
    `id` UInt32,
    `advertiser_id` String,
    `campaign_id` String,
    `name` String,
    `budget` Float64,
    `budget_mode` String,
    `landing_type` String,
    `status` String,
    `modify_time` String,
    `campaign_type` String,
    `campaign_create_time` DateTime,
    `campaign_modify_time` DateTime,
    `create_time` DateTime,
    `update_time` DateTime
)
ENGINE = MergeTree
PARTITION BY advertiser_id
ORDER BY campaign_id
SETTINGS index_granularity = 8192;

SELECT 'INSERT INTO main_table_01818';
INSERT INTO main_table_01818 SELECT 1 as `id`, 'ClickHouse' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` UInt32,
    `advertiser_id` String,
    `campaign_id` String,
    `name` String,
    `budget` Float64,
    `budget_mode` String,
    `landing_type` String,
    `status` String,
    `modify_time` String,
    `campaign_type` String,
    `campaign_create_time` DateTime,
    `campaign_modify_time` DateTime,
    `create_time` DateTime,
    `update_time` DateTime', 10, 10, 10) 
LIMIT 100;

SELECT 'INSERT INTO tmp_table_01818';
INSERT INTO tmp_table_01818 SELECT 2 as `id`, 'Database' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` UInt32,
    `advertiser_id` String,
    `campaign_id` String,
    `name` String,
    `budget` Float64,
    `budget_mode` String,
    `landing_type` String,
    `status` String,
    `modify_time` String,
    `campaign_type` String,
    `campaign_create_time` DateTime,
    `campaign_modify_time` DateTime,
    `create_time` DateTime,
    `update_time` DateTime', 10, 10, 10) 
LIMIT 100;

SELECT 'INSERT INTO tmp_table_01818';
INSERT INTO tmp_table_01818 SELECT 3 as `id`, 'ClickHouse' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` UInt32,
    `advertiser_id` String,
    `campaign_id` String,
    `name` String,
    `budget` Float64,
    `budget_mode` String,
    `landing_type` String,
    `status` String,
    `modify_time` String,
    `campaign_type` String,
    `campaign_create_time` DateTime,
    `campaign_modify_time` DateTime,
    `create_time` DateTime,
    `update_time` DateTime', 10, 10, 10) 
LIMIT 100;

SELECT 'ALL tmp_table_01818', count() FROM tmp_table_01818;
SELECT 'ALL main_table_01818', count() FROM main_table_01818;
SELECT 'tmp_table_01818', count() FROM tmp_table_01818 WHERE `advertiser_id` = 'ClickHouse';
SELECT 'main_table_01818', count() FROM main_table_01818 WHERE `advertiser_id` = 'ClickHouse';

SELECT 'Executing ALTER TABLE MOVE PARTITION...';
ALTER TABLE tmp_table_01818 MOVE PARTITION 'ClickHouse' TO TABLE main_table_01818;


SELECT 'ALL tmp_table_01818', count() FROM tmp_table_01818;
SELECT 'ALL main_table_01818', count() FROM main_table_01818;
SELECT 'tmp_table_01818', count() FROM tmp_table_01818 WHERE `advertiser_id` = 'ClickHouse';
SELECT 'main_table_01818', count() FROM main_table_01818 WHERE `advertiser_id` = 'ClickHouse';


DROP TABLE IF EXISTS main_table_01818;
DROP TABLE IF EXISTS tmp_table_01818;
