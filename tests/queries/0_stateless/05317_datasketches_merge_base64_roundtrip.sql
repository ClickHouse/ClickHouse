-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- `mergeSerializedHLL`, `mergeSerializedQuantiles` and `mergeSerializedTDigest` with
-- `base64_encoded = 1` must decode base64 text (as produced by external services and
-- stored in CSV/JSON) and give the same result as merging the raw bytes.

DROP TABLE IF EXISTS sketches_raw;
DROP TABLE IF EXISTS sketches_b64;

CREATE TABLE sketches_raw (k UInt8, hll String, quantiles String, tdigest String) ENGINE = Memory;

INSERT INTO sketches_raw
SELECT
    number % 4 AS k,
    serializedHLL(number),
    serializedQuantiles(number),
    serializedTDigest(number)
FROM numbers(10000)
GROUP BY k;

-- Base64 text, as it would arrive from an external service.
CREATE TABLE sketches_b64 (k UInt8, hll String, quantiles String, tdigest String) ENGINE = Memory;
INSERT INTO sketches_b64 SELECT k, base64Encode(hll), base64Encode(quantiles), base64Encode(tdigest) FROM sketches_raw;

SELECT 'HLL';
SELECT
    (SELECT mergeSerializedHLL(1)(hll) FROM sketches_b64) = (SELECT mergeSerializedHLL(0)(hll) FROM sketches_raw),
    (SELECT cardinalityFromHLL(mergeSerializedHLL(1)(hll)) FROM sketches_b64) BETWEEN 9000 AND 11000;

SELECT 'Quantiles';
-- KLL compaction is randomized, so the merged bytes are not compared, only the estimate.
SELECT
    (SELECT length(mergeSerializedQuantiles(1)(quantiles)) FROM sketches_b64) = (SELECT length(mergeSerializedQuantiles(0)(quantiles)) FROM sketches_raw),
    (SELECT percentileFromQuantiles(mergeSerializedQuantiles(1)(quantiles), 0.5) FROM sketches_b64) BETWEEN 4500 AND 5500;

SELECT 'TDigest';
SELECT
    (SELECT mergeSerializedTDigest(1)(tdigest) FROM sketches_b64) = (SELECT mergeSerializedTDigest(0)(tdigest) FROM sketches_raw),
    (SELECT percentileFromTDigest(mergeSerializedTDigest(1)(tdigest), 0.5) FROM sketches_b64) BETWEEN 4500 AND 5500;

DROP TABLE sketches_raw;
DROP TABLE sketches_b64;
