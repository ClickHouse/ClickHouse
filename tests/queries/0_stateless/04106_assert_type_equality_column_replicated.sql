-- Test that assertTypeEquality correctly handles the case where rhs is a ColumnReplicated
-- but this (destination) is a regular column, or vice versa.
-- This is a regression test for the fix in IColumn::assertTypeEquality that checks
-- both sides for wrapper column types (Const, Sparse, Replicated).
-- The bug manifested as a logical error exception in debug/sanitizer builds during
-- MergingSortedAlgorithm::merge when ColumnReplicated columns from JOIN
-- were mixed with regular columns.

SET enable_lazy_columns_replication = 1;
SET join_use_nulls = 1;
SET max_threads = 4;
SET max_block_size = 1024;

-- RIGHT JOIN produces ColumnReplicated for the left-side columns.
-- ORDER BY triggers MergingSortedAlgorithm to merge multiple streams.
-- The String column ensures isLazyReplicationUseful returns true.
SELECT a.generate_series, b.generate_series, length(a.x)
FROM (
    SELECT generate_series, repeat('x', generate_series % 100) AS x
    FROM generate_series(1, 5000)
) a
RIGHT JOIN (
    SELECT generate_series
    FROM generate_series(1, 5000)
) b USING (generate_series)
ORDER BY a.generate_series
FORMAT Null;
