-- Tests for quantileREQ / quantilesREQ aggregate functions backed by the DataSketches REQ sketch.
-- REQ provides relative rank error guarantees (error scales with the rank, not absolute).
-- The sketch uses HRA (High Rank Accuracy) mode: tail accuracy is prioritised.
-- REQ uses randomized compaction so individual estimates vary between runs.
-- Tests verify correctness via range checks (BETWEEN) and exact edge cases.

-- Exact max: level=1.0 always returns the maximum item.
SELECT quantileREQ(12, 1.0)(number + 1) FROM numbers(1000);

-- Median of 1..1000 must lie in [480, 520] (within 2% of true value 500).
SELECT quantileREQ(12, 0.5)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- p99 of 1..1000 must lie in [970, 1000]. REQ HRA mode is especially accurate here.
SELECT quantileREQ(12, 0.99)(number + 1) BETWEEN 970 AND 1000 FROM numbers(1000);

-- quantilesREQ: each level in the array must satisfy its range and be non-decreasing.
SELECT
    arr[1] BETWEEN 230 AND 270,
    arr[2] BETWEEN 480 AND 520,
    arr[3] BETWEEN 730 AND 770,
    arr[1] <= arr[2],
    arr[2] <= arr[3]
FROM (SELECT quantilesREQ(12, 0.25, 0.5, 0.75)(number + 1) AS arr FROM numbers(1000));

-- medianREQ alias.
SELECT medianREQ(12)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- Odd k is silently rounded down to the nearest even value (k=13 → k=12).
SELECT quantileREQ(13, 0.5)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- State round-trip: serialise then merge; numbers(49999) gives 1..49999, true median=25000.
SELECT quantileREQMerge(12, 0.5)(x) BETWEEN 24000 AND 26000
FROM
(
    SELECT quantileREQState(12, 0.5)(number + 1) AS x
    FROM numbers(49999)
);

-- Multi-shard merge: {1..500} ∪ {501..1000}, true median=500.
SELECT quantileREQMerge(12, 0.5)(x) BETWEEN 480 AND 520
FROM
(
    SELECT quantileREQState(12, 0.5)(number + 1) AS x
    FROM numbers(500)
    UNION ALL
    SELECT quantileREQState(12, 0.5)(number + 501) AS x
    FROM numbers(500)
);

-- quantilesREQMerge: multiple levels from a merged state.
SELECT
    arr[1] BETWEEN 230 AND 270,
    arr[2] BETWEEN 480 AND 520,
    arr[3] BETWEEN 730 AND 770
FROM
(
    SELECT quantilesREQMerge(12, 0.25, 0.5, 0.75)(x) AS arr
    FROM
    (
        SELECT quantilesREQState(12, 0.25, 0.5, 0.75)(number + 1) AS x
        FROM numbers(1000)
    )
);

-- quantilesREQMergeState: merge two partial states into a new state, then finalise.
SELECT quantilesREQMerge(12, 0.5)(merged)[1] BETWEEN 480 AND 520
FROM
(
    SELECT quantilesREQMergeState(12, 0.5)(x) AS merged
    FROM
    (
        SELECT quantilesREQState(12, 0.5)(number + 1) AS x
        FROM numbers(500)
        UNION ALL
        SELECT quantilesREQState(12, 0.5)(number + 501) AS x
        FROM numbers(500)
    )
);

-- Cross-variant merge: state from quantileREQState must be readable by quantilesREQMerge.
SELECT quantilesREQMerge(12, 0.25, 0.5, 0.75)(x)[2] BETWEEN 480 AND 520
FROM
(
    SELECT quantileREQState(12, 0.5)(number + 1) AS x
    FROM numbers(1000)
);

-- Multi-level sketch deserialization: numbers(10000) guarantees compaction to multiple levels.
SELECT quantilesREQMerge(12, 0.5, 0.99)(x)[1] BETWEEN 4800 AND 5200
FROM
(
    SELECT quantilesREQState(12, 0.5, 0.99)(number + 1) AS x
    FROM numbers(10000)
);

-- Edge case: empty input returns NaN.
SELECT isNaN(quantileREQ(12, 0.5)(number)) FROM numbers(0);

-- Edge case: single element — exact result.
SELECT quantileREQ(12, 0.5)(number + 42) FROM numbers(1);

-- Float32 input: median must be in range.
SELECT quantileREQ(12, 0.5)(toFloat32(number + 1)) BETWEEN toFloat32(480) AND toFloat32(520) FROM numbers(1000);

-- Negative Int32 values: single element, exact result.
SELECT quantileREQ(12, 0.5)(toInt32(number + 1) * -1) FROM numbers(1) SETTINGS enable_analyzer = 0;

-- k=4 (lower bound) is accepted.
SELECT quantileREQ(4, 0.5)(number + 1) BETWEEN 400 AND 600 FROM numbers(1000);

-- k=1024 (upper bound) is accepted.
SELECT quantileREQ(1024, 0.5)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- Error cases.
SELECT quantileREQ()(number) FROM numbers(10); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT quantileREQ(3, 0.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT quantileREQ(0.5, 0.5)(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- k above the documented upper bound (1024) is rejected before any silent narrowing.
SELECT quantileREQ(1025, 0.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
-- Large UInt64 that would silently truncate to a small `uint16_t` is rejected.
SELECT quantileREQ(70000, 0.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
