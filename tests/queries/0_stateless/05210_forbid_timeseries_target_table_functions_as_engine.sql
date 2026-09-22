SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_05210;
DROP TABLE IF EXISTS ts_ext_05210;
DROP TABLE IF EXISTS ext_samples_05210;

CREATE TABLE ts_05210 ENGINE = TimeSeries;

-- The transient form keeps working.
SELECT count() FROM timeSeriesData(ts_05210);
SELECT count() FROM timeSeriesTags(ts_05210);

-- A persistent table over one of these functions proxies the TimeSeries table's own target table.
-- The proxy renames that live table in memory, after which DROP TABLE ts_05210 resolves to the
-- proxy and drops the wrong catalog entry. Every registered name is refused, with and without an
-- explicit column list.
CREATE TABLE p_data_05210 AS timeSeriesData(ts_05210); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_samples_05210 AS timeSeriesSamples(ts_05210); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_tags_05210 AS timeSeriesTags(ts_05210); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_metric_families_05210 AS timeSeriesMetricFamilies(ts_05210); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_metrics_05210 AS timeSeriesMetrics(ts_05210); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_cols_05210 (id Tuple(UInt64, LowCardinality(UUID)), timestamp DateTime64(3), value Float64)
    AS timeSeriesData(ts_05210); -- { serverError BAD_ARGUMENTS }

-- An external target is refused the same way. Both properties the proxy asserts hold for a Memory
-- table, so nothing there would stop the in-memory rename.
CREATE TABLE ext_samples_05210 (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = Memory;
CREATE TABLE ts_ext_05210 ENGINE = TimeSeries SETTINGS version = 5 DATA ext_samples_05210;
CREATE TABLE p_ext_05210 AS timeSeriesData(ts_ext_05210); -- { serverError BAD_ARGUMENTS }

-- No persisted definition over a TimeSeries table may survive this test: such a definition can fail
-- to load on the next server start and take every unfiltered read of system.tables with it.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND engine = 'Proxy';

DROP TABLE ts_ext_05210;
DROP TABLE ext_samples_05210;
DROP TABLE ts_05210;
