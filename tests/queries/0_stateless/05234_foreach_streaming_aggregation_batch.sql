-- Tests batch aggregation for -ForEach combinators with addBatchSinglePlace and addBatch.
DROP TABLE IF EXISTS test_foreach_batch;

CREATE TABLE test_foreach_batch (
    grp UInt32,
    arr Array(Float64),
    arr_null Array(Nullable(Float64))
) ENGINE = Memory;

-- Insert batches with matching and varying array lengths.
INSERT INTO test_foreach_batch VALUES
    (1, [1.0, 2.0, 3.0], [1.0, NULL, 3.0]),
    (1, [4.0, 5.0, 6.0], [NULL, 5.0, 6.0]),
    (1, [7.0, 8.0, 9.0], [7.0, 8.0, NULL]),
    (2, [10.0, 20.0], [10.0, NULL]),
    (2, [30.0, 40.0], [30.0, 40.0]),
    (3, [], []);

SELECT grp, sumForEach(arr), minForEach(arr), maxForEach(arr), avgForEach(arr)
FROM test_foreach_batch
GROUP BY grp
ORDER BY grp;

SELECT grp, sumForEach(arr_null), minForEach(arr_null), maxForEach(arr_null), avgForEach(arr_null)
FROM test_foreach_batch
GROUP BY grp
ORDER BY grp;

-- Test single place batch aggregation without GROUP BY.
SELECT sumForEach(arr), minForEach(arr), maxForEach(arr)
FROM test_foreach_batch
WHERE grp = 1;

-- A multi-argument -ForEach must still reject a row whose arrays have different sizes. The batch
-- paths take the row boundaries from the first argument only, so they have to make the check the
-- row-at-a-time path makes; the single-argument aggregates above cannot catch that.
DROP TABLE IF EXISTS test_foreach_mismatch;
CREATE TABLE test_foreach_mismatch (grp UInt32, a Array(Float64), b Array(Float64)) ENGINE = Memory;
INSERT INTO test_foreach_mismatch VALUES (1, [1.0, 2.0], [3.0, 4.0]), (1, [1.0, 2.0], [3.0]);

-- addBatchSinglePlace (no GROUP BY) and addBatch (grouped).
SELECT corrForEach(a, b) FROM test_foreach_mismatch; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT grp, corrForEach(a, b) FROM test_foreach_mismatch GROUP BY grp; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- A filtered-out row is not validated, exactly as the row-at-a-time path leaves it unvalidated.
SELECT corrForEachIf(a, b, length(a) = length(b)) FROM test_foreach_mismatch;

DROP TABLE test_foreach_mismatch;

-- `-If` wraps `-ForEach`, so the batch paths receive a condition column. Filtered rows must neither
-- contribute nor stretch the state: the skipped row below is longer than the ones that count.
DROP TABLE IF EXISTS test_foreach_if;
CREATE TABLE test_foreach_if (grp UInt32, arr Array(Float64), cond UInt8) ENGINE = Memory;
INSERT INTO test_foreach_if VALUES (1, [1.0, 2.0, 3.0], 1), (1, [10.0, 10.0, 10.0, 10.0, 10.0], 0), (1, [4.0, 5.0, 6.0], 1), (2, [7.0, 8.0], 1), (2, [9.0, 9.0], 0);

-- addBatchSinglePlace under -If, and the same answer taken the long way round.
SELECT sumForEachIf(arr, cond) FROM test_foreach_if;
SELECT sumForEach(arr) FROM test_foreach_if WHERE cond;

-- addBatch under -If.
SELECT grp, sumForEachIf(arr, cond) FROM test_foreach_if GROUP BY grp ORDER BY grp;

-- Every row filtered out leaves an empty state, not a stretched one.
SELECT sumForEachIf(arr, 0) FROM test_foreach_if;

-- A `Nullable` condition goes through `AggregateFunctionIfNullVariadic`, which folds the condition into
-- a null map and calls addBatchSinglePlaceNotNull. A NULL condition skips the row like a false one.
SELECT sumForEachIf(arr, toNullable(cond)) FROM test_foreach_if;
SELECT sumForEachIf(arr, if(grp = 2, NULL, cond)) FROM test_foreach_if;

-- The same, grouped: `AggregateFunctionIfNullVariadic::addBatch` folds the condition into one column
-- and passes the batch on. A group whose rows are all NULL-filtered keeps an empty state.
SELECT grp, sumForEachIf(arr, toNullable(cond)) FROM test_foreach_if GROUP BY grp ORDER BY grp;
SELECT grp, sumForEachIf(arr, if(grp = 2, NULL, cond)) FROM test_foreach_if GROUP BY grp ORDER BY grp;

-- With `group_by_overflow_mode = 'any'` a key past the limit gets no place, so `Aggregator` calls
-- `addBatch` (not `addBatchWithNonNullPlaces`) and both batch paths must skip the null places.
SELECT grp, sumForEach(arr), sumForEachIf(arr, cond), sumForEachIf(arr, toNullable(cond))
FROM test_foreach_if GROUP BY grp ORDER BY grp
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'any', max_threads = 1;

DROP TABLE test_foreach_if;
DROP TABLE test_foreach_batch;

-- `AggregateFunctionIfNullVariadic::addBatch` serves every multi-argument `-If` with a `Nullable` argument
-- or condition, not only `-ForEach`. Each row is compared with the same aggregate over rows filtered in
-- `WHERE`, which never reaches the `-If` combinator. The result is `Nullable` because `x` is, and the
-- group `k = 3` has no row that passes, so it is NULL on both sides.
SELECT k,
    round(covarPopIf(x, y, c), 6) AS if_cond,
    round(covarPopIf(x, y, nc), 6) AS if_nullable_cond,
    corrIf(x, y, nc) IS NULL AS corr_is_null
FROM
(
    SELECT number % 4 AS k,
        if(number % 5 = 0, NULL, toFloat64(number)) AS x,
        toFloat64(number * number % 17) AS y,
        k != 3 AND number % 3 != 0 AS c,
        if(number % 7 = 0, NULL, c) AS nc
    FROM numbers(1000)
)
GROUP BY k ORDER BY k
SETTINGS max_block_size = 97;

SELECT k, round(covarPop(x, y), 6) AS reference_cond
FROM
(
    SELECT number % 4 AS k, if(number % 5 = 0, NULL, toFloat64(number)) AS x, toFloat64(number * number % 17) AS y
    FROM numbers(1000) WHERE k != 3 AND number % 3 != 0
)
GROUP BY k ORDER BY k;

SELECT k, round(covarPop(x, y), 6) AS reference_nullable_cond
FROM
(
    SELECT number % 4 AS k, if(number % 5 = 0, NULL, toFloat64(number)) AS x, toFloat64(number * number % 17) AS y
    FROM numbers(1000) WHERE k != 3 AND number % 3 != 0 AND number % 7 != 0
)
GROUP BY k ORDER BY k;
