DROP TABLE IF EXISTS t_merge_projections;
DROP TABLE IF EXISTS t_rng;

CREATE TABLE t_merge_projections
(
`datetime` DateTime, -- 20,000 records per second
`user_id` UInt64, -- Cardinality == 100,000,000
`device_id` UInt64, -- Cardinality == 200,000,000
`video_id` UInt64, -- Cardinality == 100,00000
`domain` LowCardinality(String), -- Cardinality == 100
`bytes` UInt64, -- Ranging from 128 to 1152
`duration` UInt64, -- Ranging from 100 to 400
PROJECTION p_norm (SELECT datetime, device_id, bytes, duration ORDER BY device_id),
PROJECTION p_agg (SELECT toStartOfHour(datetime) AS hour, domain, sum(bytes), avg(duration) GROUP BY hour, domain)
)
ENGINE = MergeTree
ORDER BY (user_id, device_id, video_id) -- Can only favor one column here
SETTINGS index_granularity = 1000;

SET max_block_size = 5000;
SET max_insert_block_size = 5000;
SET min_insert_block_size_rows = 5000;

SYSTEM STOP MERGES t_merge_projections;

CREATE TABLE t_rng (`user_id_raw` UInt64, `device_id_raw` UInt64, `video_id_raw` UInt64, `domain_raw` UInt64, `bytes_raw` UInt64, `duration_raw` UInt64) ENGINE = GenerateRandom(1024);
INSERT INTO t_merge_projections SELECT toUnixTimestamp(toDateTime(today())) + (rowNumberInAllBlocks() / 20000), user_id_raw % 100000000 AS user_id, device_id_raw % 200000000 AS device_id, video_id_raw % 100000000 AS video_id, domain_raw % 100, (bytes_raw % 1024) + 128, (duration_raw % 300) + 100 FROM t_rng LIMIT 50000;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_projections' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_projections' AND active;

SYSTEM START MERGES t_merge_projections;
OPTIMIZE TABLE t_merge_projections FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_projections' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_projections' AND active;

DROP TABLE IF EXISTS t_result_projections;
DROP TABLE IF EXISTS t_result_no_projections;

SET allow_experimental_projection_optimization = 1;
SET force_optimize_projection = 1;

CREATE TABLE t_result_projections ENGINE = Memory AS
SELECT
    toStartOfHour(datetime) AS hour, domain, sum(bytes), avg(duration)
FROM t_merge_projections
GROUP BY hour, domain
ORDER BY hour, domain;

SET allow_experimental_projection_optimization = 0;
SET force_optimize_projection = 0;

CREATE TABLE t_result_no_projections ENGINE = Memory AS
SELECT
    toStartOfHour(datetime) AS hour, domain, sum(bytes), avg(duration)
FROM t_merge_projections
GROUP BY hour, domain
ORDER BY hour, domain;

SELECT (SELECT sum(cityHash64(*)) FROM t_result_projections) = (SELECT sum(cityHash64(*)) FROM t_result_no_projections);

DROP TABLE t_merge_projections;
DROP TABLE t_rng;
DROP TABLE t_result_projections;
DROP TABLE t_result_no_projections;
