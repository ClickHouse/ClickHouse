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

DROP TABLE test_foreach_mismatch;
DROP TABLE test_foreach_batch;
