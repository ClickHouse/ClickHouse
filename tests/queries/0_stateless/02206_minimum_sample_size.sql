WITH minSampleSizeContinous(20.0, 10.0, 0.05, 0.8, 0.05) AS res SELECT res.1, roundBankers(res.2, 2), roundBankers(res.3, 2);

DROP TABLE IF EXISTS minimum_sample_size_continuos;
CREATE TABLE minimum_sample_size_continuos (baseline Float64, sigma Float64, mde Float64, power Float64, alpha Float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_continuos VALUES (20.0, 10.0, 0.05, 0.8, 0.05);
INSERT INTO minimum_sample_size_continuos VALUES (200.0, 10.0, 0.05, 0.85, 0.10);
WITH minSampleSizeContinous(baseline, sigma, mde, power, alpha) AS res SELECT res.1, roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_continuos;
DROP TABLE IF EXISTS minimum_sample_size_continuos;

WITH minSampleSizeConversion(0.9, 0.01, 0.8, 0.05) AS res SELECT res.1, roundBankers(res.2, 2), roundBankers(res.3, 2);

DROP TABLE IF EXISTS minimum_sample_size_conversion;
CREATE TABLE minimum_sample_size_conversion (p1 Float64, mdes Float64, power Float64, alpha Float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_conversion VALUES (0.9, 0.01, 0.8, 0.05);
INSERT INTO minimum_sample_size_conversion VALUES (0.8, 0.02, 0.85, 0.10);
WITH minSampleSizeConversion(p1, mdes, power, alpha) AS res SELECT res.1, roundBankers(res.2, 2), roundBankers(res.3, 2) FROM minimum_sample_size_conversion;
DROP TABLE IF EXISTS minimum_sample_size_conversion;
