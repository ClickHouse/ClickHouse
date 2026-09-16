-- Tags: no-fasttest
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `topk` and `bottomk` rank by value. A tie used to be broken by the key, which is the group number
-- and is assigned in the order the rows are read, so the answer depended on how the data was split
-- into parts or shards. They now take the same per-series sampling key `limitk` takes.

SET enable_time_series_aggregate_functions = 1;

SELECT '--- the sampling key decides a tie, whichever order the rows arrive in ---';
-- Two series tie at 1; series 1 has the larger sampling key, so series 2 wins either way.
WITH [(1::UInt64, 500::UInt64, [1.]), (2::UInt64, 100::UInt64, [1.])]::Array(Tuple(UInt64, UInt64, Array(Float64))) AS series
SELECT timeSeriesBottomKMasks(1, s.1, s.2, s.3) FROM (SELECT arrayJoin(series) AS s);
WITH [(2::UInt64, 100::UInt64, [1.]), (1::UInt64, 500::UInt64, [1.])]::Array(Tuple(UInt64, UInt64, Array(Float64))) AS series
SELECT timeSeriesBottomKMasks(1, s.1, s.2, s.3) FROM (SELECT arrayJoin(series) AS s);

SELECT '--- and the same for topk ---';
WITH [(1::UInt64, 500::UInt64, [1.]), (2::UInt64, 100::UInt64, [1.])]::Array(Tuple(UInt64, UInt64, Array(Float64))) AS series
SELECT timeSeriesTopKMasks(1, s.1, s.2, s.3) FROM (SELECT arrayJoin(series) AS s);
WITH [(2::UInt64, 100::UInt64, [1.]), (1::UInt64, 500::UInt64, [1.])]::Array(Tuple(UInt64, UInt64, Array(Float64))) AS series
SELECT timeSeriesTopKMasks(1, s.1, s.2, s.3) FROM (SELECT arrayJoin(series) AS s);

SELECT '--- a value that is not tied still decides on its own ---';
WITH [(1::UInt64, 100::UInt64, [2.]), (2::UInt64, 500::UInt64, [1.])]::Array(Tuple(UInt64, UInt64, Array(Float64))) AS series
SELECT timeSeriesBottomKMasks(1, s.1, s.2, s.3) FROM (SELECT arrayJoin(series) AS s);

SELECT '--- the sampling key is optional: without one a tie falls back to the key ---';
WITH [(1::UInt64, [1.]), (2::UInt64, [1.])]::Array(Tuple(UInt64, Array(Float64))) AS series
SELECT timeSeriesBottomKMasks(1, s.1, s.2) FROM (SELECT arrayJoin(series) AS s);

SELECT '--- rejected: limitk still requires the sampling key it ranks by ---';
SELECT timeSeriesLimitKMasks(1, 1::UInt64, [1.]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesTopKMasks(1, 1::UInt64, 'x', [1.]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- PromQL: a tie is decided the same way however the parts are laid out ---';
SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS tie_ts;
CREATE TABLE tie_ts ENGINE = TimeSeries;
SYSTEM STOP MERGES tie_ts;
-- One part per series, so the read order is not the insertion order of a single part.
INSERT INTO tie_ts (metric_name, tags, samples) VALUES ('m', map('host','h1'), [(toDateTime64(100,3), 1)]);
INSERT INTO tie_ts (metric_name, tags, samples) VALUES ('m', map('host','h2'), [(toDateTime64(100,3), 1)]);
INSERT INTO tie_ts (metric_name, tags, samples) VALUES ('m', map('host','h3'), [(toDateTime64(100,3), 2)]);
SELECT * FROM prometheusQuery(tie_ts, 'bottomk(1, m)', 100) ORDER BY ALL;
SELECT * FROM prometheusQuery(tie_ts, 'topk(2, m)', 100) ORDER BY ALL;
DROP TABLE tie_ts;
