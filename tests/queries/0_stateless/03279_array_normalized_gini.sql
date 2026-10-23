SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]);
SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2, 1]); -- { serverError ILLEGAL_COLUMN }

SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.75, 0.65, 0.6, 0.78, 0.7, 0.05, 0.4, 0.4, 0.05, 0.5, 0.1, 0.1], [1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

SELECT arrayNormalizedGini(arrayResize([1], 2000000), arrayResize([1], 2000000)); -- { serverError TOO_LARGE_ARRAY_SIZE }

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `a1` Array(Float32),
    `a2` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t VALUES ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]), ([0.9, 0.3, 0.8, 0.7], [6, 1, 0, 2]);

SELECT arrayNormalizedGini(a1, a2) FROM t;

SELECT arrayNormalizedGini(a1, [6, 1, 0, 2]) FROM t;
SELECT arrayNormalizedGini([0.9, 0.3, 0.8, 0.7], a2) FROM t;

DROP TABLE t;
