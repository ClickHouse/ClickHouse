-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- timestamp(<selector>) returns the sample timestamp for histogram samples too, and a stale histogram means no sample.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_nh;
CREATE TABLE ts_nh ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

INSERT INTO ts_nh (metric_name, tags, samples) VALUES
    ('mf', map('job', 'b'), [(toDateTime64(110, 3), 42)]),
    ('mh', map('job', 'c'), [(toDateTime64(100, 3), 3.25)]),
    ('sf', map('job', 'e'), [(toDateTime64(110, 3), reinterpretAsFloat64(toUInt64(9218868437227405314)))]);

-- flags = 16 is the stale-marker bit.
INSERT INTO ts_nh (metric_name, tags, histograms) VALUES
    ('h', map('job', 'a'), [
        (toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 1)], [4], [], [], [], 5, 1, [4], []),
        (toDateTime64(110, 3), 0, 0, 0.001, 7, 11.5, 0, [(0, 2)], [4, 3], [], [], [], 7, 0, [4, 3], [])]),
    ('mf', map('job', 'b'), [(toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 1)], [4], [], [], [], 5, 1, [4], [])]),
    ('mh', map('job', 'c'), [(toDateTime64(115, 3), 0, 0, 0.001, 7, 11.5, 0, [(0, 2)], [4, 3], [], [], [], 7, 0, [4, 3], [])]),
    ('sh', map('job', 'd'), [
        (toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 1)], [4], [], [], [], 5, 1, [4], []),
        (toDateTime64(110, 3), 16, 0, 0.001, 9, 9, 0, [(0, 1)], [9], [], [], [], 9, 0, [9], [])]),
    ('sf', map('job', 'e'), [(toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 1)], [4], [], [], [], 5, 1, [4], [])]);

SELECT '-- histogram-only series';
SELECT * FROM prometheusQuery(ts_nh, 'timestamp(h)', 120) FORMAT TSVWithNamesAndTypes;
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(h offset 15s)', 120);

SELECT '-- mixed series: the newest sample is a float, then a histogram';
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(mf)', 120);
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(mh)', 120);

SELECT '-- the newest sample is a stale histogram or a stale float: no sample';
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(sh)', 120);
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(sh)', 105);
SELECT tags, value FROM prometheusQuery(ts_nh, 'timestamp(sf)', 120);

SELECT '-- range query';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_nh, 'timestamp(h)', 100, 120, 10);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_nh, 'timestamp(sh)', 100, 120, 10);

DROP TABLE ts_nh;
