-- Tags: no-fasttest
-- Tag no-fasttest: the `TimeSeries` engine is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_threads = 1;
SET max_block_size = 256;
SET optimize_aggregation_in_order = 0;
-- Neither caller setting may make the internal read buffer all samples or lose samples under `FINAL`.
SET query_plan_join_swap_table = 1;
SET allow_aggregate_partitions_independently = 1;
SET force_aggregate_partitions_independently = 1;
SET query_plan_enable_optimizations = 1;

SELECT 'uuid';
DROP TABLE IF EXISTS ts_stream_uuid;
CREATE TABLE ts_stream_uuid ENGINE = TimeSeries
SETTINGS version = 0, recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS (id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)), all_tags Map(String, String))
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp);

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_uuid (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_uuid (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_uuid LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_uuid LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_uuid;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_uuid FINAL;

DROP TABLE ts_stream_uuid;

SELECT 'tuple';
DROP TABLE IF EXISTS ts_stream_tuple;
CREATE TABLE ts_stream_tuple ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 345600
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768;

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_tuple (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_tuple (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_tuple LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_tuple LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_tuple;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_tuple FINAL;

DROP TABLE ts_stream_tuple;

SELECT 'tuple_elements';
DROP TABLE IF EXISTS ts_stream_tuple_elements;
CREATE TABLE ts_stream_tuple_elements ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree ORDER BY (id.1, id.2, timestamp);

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_tuple_elements (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_tuple_elements (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_tuple_elements LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_tuple_elements LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_tuple_elements;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_tuple_elements FINAL;

DROP TABLE ts_stream_tuple_elements;

SELECT 'time_order';
DROP TABLE IF EXISTS ts_stream_time_order;
CREATE TABLE ts_stream_time_order ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree PARTITION BY toYYYYMM(timestamp) ORDER BY (timestamp, id);

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_time_order (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_time_order (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_time_order LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_time_order LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_time_order;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_time_order FINAL;

DROP TABLE ts_stream_time_order;

SELECT 'unordered';
DROP TABLE IF EXISTS ts_stream_unordered;
CREATE TABLE ts_stream_unordered ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = MergeTree ORDER BY tuple();

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_unordered (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_unordered (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_unordered LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_unordered LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_unordered;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_unordered FINAL;

DROP TABLE ts_stream_unordered;

SELECT 'scalar';
DROP TABLE IF EXISTS ts_stream_scalar;
CREATE TABLE ts_stream_scalar ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, tags))
SAMPLES INNER ENGINE = MergeTree ORDER BY timestamp;

-- Two batches of one series exercise both cross-block and cross-part assembly.
INSERT INTO ts_stream_scalar (metric_name, tags, time_series, metric_family, type, unit, help)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1704067200000 + i), toFloat64(i)), range(32768)),
    'm', 'gauge', 'seconds', 'description';
INSERT INTO ts_stream_scalar (metric_name, tags, time_series)
SELECT 'm', map('n', 'a'),
    arrayMap(i -> (fromUnixTimestamp64Milli(1706745600000 + i), toFloat64(i)), range(32768));

-- A fragment fits one input block (at most one 32768-row part here), whereas a full series has 65536 samples.
-- This also checks both joins with a caller forcing a swap.
SELECT length(time_series) BETWEEN 1 AND 32768, metric_name, tags['n'], metric_family, type, unit, help
FROM ts_stream_scalar LIMIT 1;
SELECT length(time_series) BETWEEN 1 AND 32768 FROM ts_stream_scalar LIMIT 1;

-- Reassembling the fragments preserves every timestamp/value pair exactly once.
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))),
    min(arrayAll(s -> toUnixTimestamp64Milli(s.1) % 100000 = s.2, time_series))
FROM ts_stream_scalar;
SELECT metric_name, tags['n'], length(time_series), arraySum(arrayMap(s -> s.2, time_series))
FROM ts_stream_scalar FINAL;

DROP TABLE ts_stream_scalar;
