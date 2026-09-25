-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_hist;
DROP TABLE IF EXISTS ts_hist_v5;
DROP TABLE IF EXISTS ts_hist_copy;
DROP TABLE IF EXISTS ts_hist_granularity;

CREATE TABLE ts_hist ENGINE = TimeSeries;

SELECT '--- a new table has version 7 and a generated histograms table ---';
SELECT extract(create_table_query, 'version = (\d+)'),
       position(create_table_query, 'HISTOGRAMS INNER COLUMNS') > 0,
       position(create_table_query, 'HISTOGRAMS INNER ENGINE') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist';

SELECT name, type, compression_codec FROM system.columns
    WHERE database = currentDatabase()
      AND table = (SELECT concat('.inner_id.histograms.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist')
    ORDER BY position;

SELECT engine, sorting_key FROM system.tables
    WHERE database = currentDatabase()
      AND name = (SELECT concat('.inner_id.histograms.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist');

SELECT count() FROM timeSeriesHistograms(ts_hist);
SELECT count() FROM timeSeriesHistograms(currentDatabase(), 'ts_hist');

SELECT '--- the index granularity settings of the histograms table and of the samples table are independent ---';
CREATE TABLE ts_hist_granularity ENGINE = TimeSeries SETTINGS histograms_index_granularity = 4096, samples_index_granularity = 1024;
SELECT extractAll(name, '\.inner_id\.(\w+)\.')[1] AS inner_table, extract(engine_full, 'index_granularity = (\d+)') AS index_granularity FROM system.tables
    WHERE database = currentDatabase()
      AND name IN (SELECT concat('.inner_id.', kind, '.', toString(uuid)) FROM system.tables ARRAY JOIN ['histograms', 'samples'] AS kind
                   WHERE database = currentDatabase() AND name = 'ts_hist_granularity')
    ORDER BY inner_table;
DROP TABLE ts_hist_granularity;

SELECT '--- the samples still work: INSERT and PromQL ---';
INSERT INTO ts_hist (metric_name, tags, samples) VALUES ('up', {'job': 'api'}, [(1000, 1.)]);
SELECT value FROM prometheusQuery(ts_hist, 'up', 1000);

SELECT '--- a table of version 5 has no histograms table ---';
CREATE TABLE ts_hist_v5 ENGINE = TimeSeries SETTINGS version = 5;
SELECT position(create_table_query, 'HISTOGRAMS') > 0 FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist_v5';
SELECT count() FROM system.tables
    WHERE database = currentDatabase()
      AND name = (SELECT concat('.inner_id.histograms.', toString(uuid)) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist_v5');
SELECT count() FROM timeSeriesHistograms(ts_hist_v5); -- { serverError UNKNOWN_TABLE }
CREATE TABLE ts_hist_granularity ENGINE = TimeSeries SETTINGS version = 5, histograms_index_granularity = 4096; -- { serverError INVALID_SETTING_VALUE }

SELECT '--- the histograms table cannot be customized yet ---';
CREATE TABLE ts_hist_explicit ENGINE = TimeSeries HISTOGRAMS ENGINE = MergeTree ORDER BY id; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE ts_hist_explicit ENGINE = TimeSeries HISTOGRAMS INNER COLUMNS (extra UInt8); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE ts_hist_external ENGINE = MergeTree ORDER BY (id, timestamp) AS SELECT * FROM timeSeriesHistograms(ts_hist);
CREATE TABLE ts_hist_explicit ENGINE = TimeSeries HISTOGRAMS ts_hist_external; -- { serverError NOT_IMPLEMENTED }
DROP TABLE ts_hist_external;

SELECT '--- a copy of a table of version 5 gets the latest version and a histograms table ---';
CREATE TABLE ts_hist_copy AS ts_hist_v5;
SELECT extract(create_table_query, 'version = (\d+)'), position(create_table_query, 'HISTOGRAMS INNER COLUMNS') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist_copy';
SELECT count() FROM timeSeriesHistograms(ts_hist_copy);
DROP TABLE ts_hist_copy;

SELECT '--- TRUNCATE, RENAME and DROP handle the histograms table ---';
TRUNCATE TABLE ts_hist;
RENAME TABLE ts_hist TO ts_hist_renamed;
SELECT count() FROM timeSeriesHistograms(ts_hist_renamed);
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.histograms.%';
DROP TABLE ts_hist_renamed;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.histograms.%';

DROP TABLE ts_hist_v5;
