-- `bitmapTransform` with matching from/to array sizes must not raise `ILLEGAL_TYPE_OF_ARGUMENT`.
-- Check this query's own query_log row instead of the process-wide system.errors counter, which
-- any concurrent test triggering the same (very common) error code would perturb. The query is
-- found by its `log_comment`, which survives comment attachment in the logged query text.

-- The bitmap is built from `Array(UInt32)` so that `888` fits into its element type: a replacement
-- value that does not fit raises `BAD_ARGUMENTS`, which would mask what this test checks.
SELECT bitmapToArray(bitmapTransform(bitmapBuild(cast([1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as Array(UInt32))), cast([5,999,2] as Array(UInt32)), cast([2,888,20] as Array(UInt32)))) AS res FORMAT Null SETTINGS log_comment = '03272_bitmap_transform';

SYSTEM FLUSH LOGS query_log;

SELECT exception_code = 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type != 'QueryStart'
    AND log_comment = '03272_bitmap_transform';
