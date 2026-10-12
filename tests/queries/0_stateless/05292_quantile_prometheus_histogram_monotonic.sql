-- Like Prometheus, a decreasing cumulative bucket value is raised to the previous one, a tiny float
-- increase is ignored, and a histogram without observations gives NaN.

SELECT 'decreasing bucket is raised to the previous one';
SELECT quantilePrometheusHistogram(0.85)(le, count) FROM VALUES('le Float64, count Float64', (1, 10), (2, 8), (inf, 10));
SELECT quantilePrometheusHistogram(0.85)(le, count) FROM VALUES('le Float64, count UInt64', (1, 10), (2, 8), (inf, 10));
SELECT quantilesPrometheusHistogram(0.5, 0.85)(le, count) FROM VALUES('le Float64, count Float64', (1, 10), (2, 8), (inf, 10));

SELECT 'decreasing +Inf bucket';
SELECT quantilesPrometheusHistogram(0.01, 0.5, 0.99)(le, count)
FROM VALUES('le Float64, count Float64', (0.1, 20), (1, 10), (10, 50), (100, 40), (1000, 90), (inf, 80));

SELECT 'tiny float increase is ignored, an integer or a large one is not';
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count Float64', (1, 100), (2, 100), (inf, 100.00000000001));
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count UInt64', (1, 10000000000000), (2, 10000000000000), (inf, 10000000000001));
SELECT quantilePrometheusHistogram(0.5)(le, count) FROM VALUES('le Float64, count Float64', (1, 1e308), (2, 1.7e308), (inf, 1.7e308));

SELECT 'no observations';
SELECT quantilePrometheusHistogram(1)(le, count) FROM VALUES('le Float64, count Float64', (0, 0), (1, 0), (2, 0), (inf, 0));
SELECT quantilesPrometheusHistogram(0, 1)(le, count) FROM VALUES('le Float64, count UInt64', (0, 0), (1, 0), (inf, 0));
