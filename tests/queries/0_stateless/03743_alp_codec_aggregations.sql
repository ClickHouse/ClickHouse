SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS base32;
CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS base64;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp32;
CREATE TABLE alp32 (i UInt32 CODEC(NONE), f Float32 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64;
CREATE TABLE alp64 (i UInt32 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64_granule2048;
CREATE TABLE alp64_granule2048 (i UInt64 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i SETTINGS index_granularity = 2048;


SELECT '# Aggregations Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(5000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT count(), abs(sum(a.f) - sum(b.f)) < 0.0001, abs(avg(a.f) - avg(b.f)) < 0.0001, min(a.f) = min(b.f), max(a.f) = max(b.f)
FROM base64 AS b INNER JOIN alp64 AS a USING i
WHERE a.i BETWEEN 1500 AND 3000;

TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, toFloat32(round(number * 0.1 + cos(number), 2)) FROM numbers(5000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), abs(sum(a.f) - sum(b.f)) < 0.0001, abs(avg(a.f) - avg(b.f)) < 0.0001, min(a.f) = min(b.f), max(a.f) = max(b.f)
FROM base32 AS b INNER JOIN alp32 AS a USING i
WHERE a.i BETWEEN 1500 AND 3000;


SELECT '# Granule Test';
TRUNCATE TABLE alp64_granule2048;
INSERT INTO alp64_granule2048 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(6000);
OPTIMIZE TABLE alp64_granule2048 FINAL;

SELECT 'First Granule', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i < 2048;
SELECT 'Middle granules', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i >= 2048 AND i < 4096;
SELECT 'Last granule', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i >= 4096;
SELECT 'Sparse read', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i % 1500 = 0;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
DROP TABLE alp64_granule2048;
