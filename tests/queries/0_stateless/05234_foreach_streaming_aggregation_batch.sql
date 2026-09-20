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

DROP TABLE test_foreach_batch;
