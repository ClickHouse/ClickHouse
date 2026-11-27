-- Tags: no-replicated-database
-- ^ due to the usage of ON CLUSTER queries

SET distributed_ddl_output_mode = 'none', enable_analyzer = true;

drop table if exists source, mview;

CREATE TABLE source
(
    timestamp DateTime,
    card_id UInt64,
    _id String
)
ENGINE = MergeTree Partition by toYYYYMM(timestamp)
ORDER BY _id TTL toDateTime(timestamp + toIntervalDay(7));

CREATE MATERIALIZED VIEW mview on cluster test_shard_localhost
ENGINE =  SummingMergeTree ORDER BY (day, card_id)
as SELECT
    toDate(timestamp) AS day,
    card_id,
    count(*) AS card_view
FROM source GROUP BY (day, card_id);

DROP TABLE mview;

CREATE MATERIALIZED VIEW mview on cluster test_shard_localhost
(
    day Date,
    card_id UInt64,
    card_view Int64
)
ENGINE =  SummingMergeTree ORDER BY (day, card_id)
as SELECT
    toDate(timestamp) AS day,
    card_id,
    count(*) AS card_view
FROM source GROUP BY (day, card_id);

alter table source on cluster test_shard_localhost MODIFY SETTING ttl_only_drop_parts = 1;

drop table if exists mview, source;
