-- FORMAT OpenMetrics: histogram `le` and summary `quantile` label values are validated on input and
-- output, and the writer orders histogram / summary samples canonically regardless of the row order.

SET session_timezone = 'UTC';

-- ===== Output: samples are ordered by label set, boundary value, and suffix =====

-- Buckets in a non-numeric order (and `+Inf` first), with `_sum` before `_count`, for two label sets.
SELECT metric_name, 'h' AS metric_family, 'histogram' AS type, tags, [(fromUnixTimestamp64Milli(toInt64(0)), v)] AS samples FROM
(
    SELECT 1 AS n, 'h_bucket' AS metric_name, [('le', '+Inf'), ('m', 'a')]::Array(Tuple(String, String)) AS tags, 5.0 AS v
    UNION ALL SELECT 2, 'h_sum', [('m', 'a')], 9.5
    UNION ALL SELECT 3, 'h_bucket', [('le', '10'), ('m', 'a')], 4.0
    UNION ALL SELECT 4, 'h_bucket', [('le', '2.5'), ('m', 'a')], 1.0
    UNION ALL SELECT 5, 'h_count', [('m', 'a')], 5.0
    UNION ALL SELECT 6, 'h_bucket', [('m', 'b'), ('le', '1e-1')], 0.0
    UNION ALL SELECT 7, 'h_count', [('m', 'b')], 1.0
    UNION ALL SELECT 8, 'h_bucket', [('m', 'b'), ('le', '+Inf')], 1.0
    UNION ALL SELECT 9, 'h_bucket', [('le', '-1'), ('m', 'a')], 0.0
) ORDER BY n FORMAT OpenMetrics;

-- Quantiles in a lexicographic order that differs from the numeric one, with `_count` first.
SELECT metric_name, 's' AS metric_family, 'summary' AS type, tags, [(fromUnixTimestamp64Milli(toInt64(0)), v)] AS samples FROM
(
    SELECT 1 AS n, 's_count' AS metric_name, []::Array(Tuple(String, String)) AS tags, 10.0 AS v
    UNION ALL SELECT 2, 's', [('quantile', '0.99')], 9.0
    UNION ALL SELECT 3, 's', [('quantile', '0.5')], 5.0
    UNION ALL SELECT 4, 's', [('quantile', '0.05')], 1.0
    UNION ALL SELECT 5, 's_sum', [], 50.0
) ORDER BY metric_name DESC, tags FORMAT OpenMetrics;

-- ===== Output: invalid boundary values are rejected =====

SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [('le', 'NaN')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [('le', 'bogus')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h_bucket' AS metric_name, 'h' AS metric_family, 'histogram' AS type, [('le', '')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS metric_name, 's' AS metric_family, 'summary' AS type, [('quantile', '2')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS metric_name, 's' AS metric_family, 'summary' AS type, [('quantile', '-0.1')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS metric_name, 's' AS metric_family, 'summary' AS type, [('quantile', 'bogus')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS samples FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== Input: invalid boundary values are rejected =====

SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="NaN"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="x"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's{quantile="2"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's{quantile="NaN"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- Valid boundary values are accepted.
SELECT metric_name, tags FROM format(OpenMetrics, concat(
    '# TYPE h histogram', char(10),
    'h_bucket{le="0.5"} 1', char(10),
    'h_bucket{le="+Inf"} 2', char(10),
    '# TYPE s summary', char(10),
    's{quantile="0"} 1', char(10),
    's{quantile="1"} 2', char(10),
    '# EOF', char(10))) ORDER BY metric_name, tags FORMAT TSV;
