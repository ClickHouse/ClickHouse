-- FORMAT OpenMetrics: metric family shape rules shared by the reader and the writer, the `samples` /
-- `time_series` column names of the TimeSeries engine, and DateTime64 scales other than 3.

SET session_timezone = 'UTC';

-- ===== Output: sample names must follow the family type's suffix rule =====

-- Valid histogram, summary, gaugehistogram, and info families.
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, [('le', '1')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 2.0)] AS samples
    UNION ALL SELECT 2, 'h_bucket', [('le', '+Inf')], [(fromUnixTimestamp64Milli(toInt64(0)), 3.0)]
    UNION ALL SELECT 3, 'h_count', [], [(fromUnixTimestamp64Milli(toInt64(0)), 3.0)]
    UNION ALL SELECT 4, 'h_sum', [], [(fromUnixTimestamp64Milli(toInt64(0)), 1.5)]
) ORDER BY n FORMAT OpenMetrics;
SELECT metric_name, 's' AS metric_family, 'summary' AS type, tags, samples FROM
(
    SELECT 1 AS n, 's' AS metric_name, [('quantile', '0.5')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 7.0)] AS samples
    UNION ALL SELECT 2, 's_count', [], [(fromUnixTimestamp64Milli(toInt64(0)), 10.0)]
) ORDER BY n FORMAT OpenMetrics;
SELECT 'g_gcount' AS metric_name, 'g' AS metric_family, 'gaugehistogram' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics;
SELECT 'build_info' AS metric_name, 'build' AS metric_family, 'info' AS type, [('version', '1')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics;

-- A histogram sample without a suffix, with a suffix of another type, or of another family is rejected.
SELECT 'h' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [('le', '1')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h_gsum' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'other_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [('le', '1')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- A counter sample of another family is rejected, not only one without `_total`.
SELECT 'other_total' AS metric_name, 'c' AS metric_family, 'counter' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- An info sample must end with `_info`.
SELECT 'build' AS metric_name, 'build' AS metric_family, 'info' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- Histogram buckets need `le`, bare summary samples need `quantile`.
SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS metric_name, 's' AS metric_family, 'summary' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== Input: metadata and label value validation =====

-- An unknown `# TYPE` token is rejected; `untyped` is still accepted (and normalized to `unknown`).
SELECT * FROM format(OpenMetrics, concat('# TYPE m counterz', char(10), 'm 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT type FROM format(OpenMetrics, concat('# TYPE m untyped', char(10), 'm 1', char(10), '# EOF', char(10))) FORMAT TSV;
-- Invalid metric family names in `# HELP`, `# TYPE`, and `# UNIT` are rejected, even without samples.
SELECT * FROM format(OpenMetrics, concat('# TYPE 1bad gauge', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# HELP bad-name text', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# UNIT bad.name seconds', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- A raw control character inside a quoted label value is rejected (it cannot be written back).
SELECT * FROM format(OpenMetrics, concat('m{k="a', char(9), 'b"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- The `\n` escape is still decoded.
SELECT tags FROM format(OpenMetrics, concat('m{k="a\\nb"} 1', char(10), '# EOF', char(10))) FORMAT TSV;
-- A histogram bucket without `le` is rejected; one with `le` folds into the family.
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT metric_name, metric_family, tags FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="1"} 1', char(10), '# EOF', char(10))) FORMAT TSV;

-- ===== `samples` and `time_series` column names =====

-- The inferred schema names the points column `samples`, as in current TimeSeries tables.
DESCRIBE TABLE format(OpenMetrics, concat('m 1', char(10), '# EOF', char(10))) FORMAT TSV;
-- `time_series` (TimeSeries tables of version 2 and earlier) is accepted on input and output.
SELECT time_series FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 2', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT 'm' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(2000)), 1.0)] AS time_series FORMAT OpenMetrics;
-- Both at once are ambiguous.
SELECT * FROM format(OpenMetrics, 'metric_name String, samples Array(Tuple(DateTime64(3), Float64)), time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1', char(10), '# EOF', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples, samples AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== DateTime64 scales other than 3 =====

-- Points are converted between the column scale and the millisecond wire precision.
SELECT samples FROM format(OpenMetrics, 'metric_name String, samples Array(Tuple(DateTime64(6), Float64))', concat('m 1 1.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT samples FROM format(OpenMetrics, 'metric_name String, samples Array(Tuple(DateTime64(0), Float64))', concat('m 1 1.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT 'm' AS metric_name, [(toDateTime64('1970-01-01 00:00:01.234567', 6), 1.0)] AS samples FORMAT OpenMetrics;
-- A timestamp that fits Int64 milliseconds but not the finer column scale is rejected instead of overflowing.
SELECT * FROM format(OpenMetrics, 'metric_name String, samples Array(Tuple(DateTime64(9), Float64))', concat('m 1 9223372036854775.807', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
