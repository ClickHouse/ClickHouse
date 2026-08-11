-- A TimeSeries table forwards every write through nested INSERTs into its target tables
-- (TimeSeriesSink), and the outer chunk's DeduplicationInfo does not survive that hop. Two
-- materialized views converging on the same TimeSeries table therefore must not deduplicate each
-- other's blocks on a deduplicating data table, even when the blocks they produce are identical:
-- the branches belong to distinct views of one query, and deduplication between them would
-- silently lose one branch's rows. Writes arriving through TimeSeriesSink do not deduplicate on
-- the inner tables at all, so both branches land in full - this test pins that no rows are lost.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_dedup_mv_a;
DROP TABLE IF EXISTS ts_dedup_mv_b;
DROP TABLE IF EXISTS ts_dedup;
DROP TABLE IF EXISTS ts_dedup_source;
DROP TABLE IF EXISTS ts_dedup_data;
DROP TABLE IF EXISTS ts_dedup_tags;
DROP TABLE IF EXISTS ts_dedup_metrics;

CREATE TABLE ts_dedup_data (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE ts_dedup_tags (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ts_dedup_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts_dedup ENGINE = TimeSeries DATA ts_dedup_data TAGS ts_dedup_tags METRICS ts_dedup_metrics;
CREATE TABLE ts_dedup_source (x UInt64) ENGINE = MergeTree ORDER BY x;

-- The two views are identical on purpose: each branch pushes the same blocks into the same data
-- table, so if the branches shared deduplication ids, one of them would be silently dropped.
CREATE MATERIALIZED VIEW ts_dedup_mv_a TO ts_dedup AS SELECT 'metric' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x, 3), toFloat64(x))] AS time_series FROM ts_dedup_source;
CREATE MATERIALIZED VIEW ts_dedup_mv_b TO ts_dedup AS SELECT 'metric' AS metric_name, map('x', toString(x)) AS tags, [(toDateTime64(x, 3), toFloat64(x))] AS time_series FROM ts_dedup_source;

INSERT INTO ts_dedup_source SELECT number FROM numbers(4) SETTINGS parallel_view_processing = 1, deduplicate_blocks_in_dependent_materialized_views = 1, insert_deduplicate = 1, max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

-- Both branches in full: 4 rows from each of the two views, nothing deduplicated between them.
SELECT count(), uniqExact((id, timestamp, value)) FROM ts_dedup_data;

DROP TABLE ts_dedup_mv_a;
DROP TABLE ts_dedup_mv_b;
DROP TABLE ts_dedup;
DROP TABLE ts_dedup_source;
DROP TABLE ts_dedup_data;
DROP TABLE ts_dedup_tags;
DROP TABLE ts_dedup_metrics;
