-- Tests for quantileKLL / quantilesKLL aggregate functions backed by the DataSketches KLL sketch.
-- KLL uses randomized compaction so individual estimates vary between runs.
-- Tests verify correctness via range checks (BETWEEN) and exact edge cases.

-- Exact max: level=1.0 always returns the maximum retained item (sorted view last entry).
SELECT quantileKLL(200, 1.0)(number + 1) FROM numbers(1000);

-- Median of 1..1000 must lie in [480, 520] (within 2% of true value 500).
SELECT quantileKLL(200, 0.5)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- p99 of 1..1000 must lie in [970, 1000] (within ~2% of true value 990).
SELECT quantileKLL(200, 0.99)(number + 1) BETWEEN 970 AND 1000 FROM numbers(1000);

-- quantilesKLL: each level in the array must satisfy its range and be non-decreasing.
SELECT
    arr[1] BETWEEN 230 AND 270,
    arr[2] BETWEEN 480 AND 520,
    arr[3] BETWEEN 730 AND 770,
    arr[1] <= arr[2],
    arr[2] <= arr[3]
FROM (SELECT quantilesKLL(200, 0.25, 0.5, 0.75)(number + 1) AS arr FROM numbers(1000));

-- medianKLL alias produces the same result as quantileKLL with level=0.5.
SELECT medianKLL(200)(number + 1) BETWEEN 480 AND 520 FROM numbers(1000);

-- State round-trip: serialise then merge; numbers(49999) gives 1..49999, true median=25000.
SELECT quantileKLLMerge(200, 0.5)(x) BETWEEN 24000 AND 26000
FROM
(
    SELECT quantileKLLState(200, 0.5)(number + 1) AS x
    FROM numbers(49999)
);

-- Multi-shard merge: {1..500} ∪ {501..1000}, true median=500.
SELECT quantileKLLMerge(200, 0.5)(x) BETWEEN 480 AND 520
FROM
(
    SELECT quantileKLLState(200, 0.5)(number + 1) AS x
    FROM numbers(500)
    UNION ALL
    SELECT quantileKLLState(200, 0.5)(number + 501) AS x
    FROM numbers(500)
);

-- quantilesKLLMerge: multiple levels from a merged state.
SELECT
    arr[1] BETWEEN 230 AND 270,
    arr[2] BETWEEN 480 AND 520,
    arr[3] BETWEEN 730 AND 770
FROM
(
    SELECT quantilesKLLMerge(200, 0.25, 0.5, 0.75)(x) AS arr
    FROM
    (
        SELECT quantilesKLLState(200, 0.25, 0.5, 0.75)(number + 1) AS x
        FROM numbers(1000)
    )
);

-- quantilesKLLMergeState: merge two partial states into a new state, then finalise.
SELECT quantilesKLLMerge(200, 0.5)(merged)[1] BETWEEN 480 AND 520
FROM
(
    SELECT quantilesKLLMergeState(200, 0.5)(x) AS merged
    FROM
    (
        SELECT quantilesKLLState(200, 0.5)(number + 1) AS x
        FROM numbers(500)
        UNION ALL
        SELECT quantilesKLLState(200, 0.5)(number + 501) AS x
        FROM numbers(500)
    )
);

-- Edge case: empty input returns NaN.
SELECT isNaN(quantileKLL(200, 0.5)(number)) FROM numbers(0);

-- Edge case: single element — exact result.
SELECT quantileKLL(200, 0.5)(number + 42) FROM numbers(1);

-- Float32 input: median must be in range.
SELECT quantileKLL(200, 0.5)(toFloat32(number + 1)) BETWEEN toFloat32(480) AND toFloat32(520) FROM numbers(1000);

-- Negative Int32 values: single element, exact result.
SELECT quantileKLL(200, 0.5)(toInt32(number + 1) * -1) FROM numbers(1) SETTINGS enable_analyzer = 0;

-- Cross-variant merge: state from quantileKLLState must be readable by quantilesKLLMerge.
-- This tests the haveSameStateRepresentation fix (quantileKLL and quantilesKLL share state).
SELECT quantilesKLLMerge(200, 0.25, 0.5, 0.75)(x)[2] BETWEEN 480 AND 520
FROM
(
    SELECT quantileKLLState(200, 0.5)(number + 1) AS x
    FROM numbers(1000)
);

-- Multi-item deserialization path (non-single-item sketch): exercises the optional<T>
-- emplace() fix in kll_sketch_impl.hpp. numbers(10000) guarantees compaction occurs.
SELECT quantilesKLLMerge(200, 0.5, 0.99)(x)[1] BETWEEN 4800 AND 5200
FROM
(
    SELECT quantilesKLLState(200, 0.5, 0.99)(number + 1) AS x
    FROM numbers(10000)
);

-- Error cases.
SELECT quantileKLL()(number) FROM numbers(10); -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
SELECT quantileKLL(7, 0.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT quantileKLL(0.5, 0.5)(number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
