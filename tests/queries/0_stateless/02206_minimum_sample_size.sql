WITH minSampleSizeContinous(20, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous const 1', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);
WITH minSampleSizeContinous(0.0, 10.0, 0.05, 0.8, 0.05) AS res SELECT 'continous const 2', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);
WITH minSampleSizeContinous(20, 10.0, 0.05, 0.8, 0.05) AS res SELECT 'continous const 3', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);
WITH minSampleSizeContinous(20.0, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous const 4', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);

DROP TABLE IF EXISTS minimum_sample_size_continuos;
CREATE TABLE minimum_sample_size_continuos (baseline UInt64, sigma UInt64) ENGINE = Memory();
INSERT INTO minimum_sample_size_continuos VALUES (20, 10);
INSERT INTO minimum_sample_size_continuos VALUES (200, 10);
WITH minSampleSizeContinous(baseline, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous UInt64 1', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
WITH minSampleSizeContinous(20, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous UInt64 2', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
WITH minSampleSizeContinous(baseline, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous UInt64 3', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
DROP TABLE IF EXISTS minimum_sample_size_continuos;

DROP TABLE IF EXISTS minimum_sample_size_continuos;
CREATE TABLE minimum_sample_size_continuos (baseline Float64, sigma Float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_continuos VALUES (20, 10);
INSERT INTO minimum_sample_size_continuos VALUES (200, 10);
WITH minSampleSizeContinous(baseline, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous Float64 1', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
WITH minSampleSizeContinous(20, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous Float64 2', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
WITH minSampleSizeContinous(baseline, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous UInt64 3', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY roundBankers(res.1, 2);
DROP TABLE IF EXISTS minimum_sample_size_continuos;

WITH minSampleSizeConversion(0.9, 0.01, 0.8, 0.05) AS res SELECT 'conversion const 1', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);
WITH minSampleSizeConversion(0.0, 0.01, 0.8, 0.05) AS res SELECT 'conversion const 2', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2);

DROP TABLE IF EXISTS minimum_sample_size_conversion;
CREATE TABLE minimum_sample_size_conversion (p1 Float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_conversion VALUES (0.9);
INSERT INTO minimum_sample_size_conversion VALUES (0.8);
WITH minSampleSizeConversion(p1, 0.01, 0.8, 0.05) AS res SELECT 'conversion Float64 1', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_conversion ORDER BY roundBankers(res.1, 2);
WITH minSampleSizeConversion(0.9, 0.01, 0.8, 0.05) AS res SELECT 'conversion Float64 2', roundBankers(res.1, 2), roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_conversion ORDER BY roundBankers(res.1, 2);
DROP TABLE IF EXISTS minimum_sample_size_conversion;
