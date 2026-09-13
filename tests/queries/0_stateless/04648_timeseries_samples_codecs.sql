-- Tags: no-replicated-database
-- Tag no-replicated-database: the DETACH/ATTACH round-trip below hangs in DatabaseReplicated mode
-- because ATTACH TABLE with a TimeSeries engine goes through the replicated DDL log and requires
-- replica sync (same as 04146_timeseries_attach_detach.sql).

-- Auto-created `timestamp` and `value` columns of the TimeSeries samples inner table
-- get compression codecs (DoubleDelta + ZSTD for timestamps, plain ZSTD for values);
-- explicitly declared columns keep the user's codecs (or none), and the normalized
-- table round-trips through DETACH/ATTACH unchanged.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_codecs;
CREATE TABLE ts_codecs ENGINE = TimeSeries;

SELECT 'default codecs:';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

-- The codecs survive a DETACH/ATTACH round-trip and are not applied twice.
DETACH TABLE ts_codecs;
ATTACH TABLE ts_codecs;

SELECT 'after detach/attach:';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_codecs;

-- Explicitly declared samples columns keep the user's choice (here: no codec).
CREATE TABLE ts_explicit ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id UUID, timestamp DateTime64(3), value Float64)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp);

SELECT 'explicit columns keep user codecs:';
SELECT name, type, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' ORDER BY position;

DROP TABLE ts_explicit;
