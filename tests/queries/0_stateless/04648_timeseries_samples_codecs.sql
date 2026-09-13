-- Tags: no-replicated-database
-- Tag no-replicated-database: the DETACH/ATTACH round-trip below hangs in DatabaseReplicated mode
-- because ATTACH TABLE with a TimeSeries engine goes through the replicated DDL log and requires
-- replica sync (same as 04146_timeseries_attach_detach.sql).

-- The auto-created columns of the TimeSeries samples inner table get compression codecs: the `timestamp` element of
-- `samples`, `bucket`, `min_time` and `max_time` get DoubleDelta, ZSTD(1), the `value` element of `samples` gets ZSTD(3).
-- The codecs of the tuple elements are allowed by `allow_experimental_time_series_table` alone, without
-- `enable_tuple_element_codecs`. Explicitly declared columns keep the user's codecs (or none), and the normalized
-- table round-trips through DETACH/ATTACH unchanged.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_codecs;
CREATE TABLE ts_codecs ENGINE = TimeSeries;

-- The samples columns of the TimeSeries table, the columns of the samples inner table, and their codecs in system.columns
-- (which shows the codecs declared for whole columns only, so `samples` has none there).
SELECT 'default codecs:';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_codecs';
SELECT extract(create_table_query, '^CREATE TABLE [^ ]+ \((.*)\) ENGINE = ')
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

-- The codecs survive a DETACH/ATTACH round-trip and are not applied twice.
DETACH TABLE ts_codecs;
ATTACH TABLE ts_codecs;

SELECT 'after detach/attach:';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_codecs';
SELECT extract(create_table_query, '^CREATE TABLE [^ ]+ \((.*)\) ENGINE = ')
FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_codecs;

-- An explicitly declared samples column keeps the user's choice (here: no codec), the other columns are generated with codecs.
DROP TABLE IF EXISTS ts_explicit;
CREATE TABLE ts_explicit ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id UUID, samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))))
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket);

SELECT 'explicit columns keep user codecs:';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_explicit';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_explicit;

-- The columns of a non-MergeTree inner table are generated without codecs: such engines ignore codecs, and they don't
-- support the codecs of tuple elements.
DROP TABLE IF EXISTS ts_memory;
CREATE TABLE ts_memory ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES INNER ENGINE = Memory;

SELECT 'no codecs for a Memory inner table:';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_memory';

DROP TABLE ts_memory;

-- Explicitly declared codecs of the tuple elements are kept as well.
CREATE TABLE ts_explicit ENGINE = TimeSeries
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3) CODEC(Delta, LZ4), value Float64 CODEC(LZ4)))));

SELECT 'explicit tuple element codecs:';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_explicit';

DROP TABLE ts_explicit;
