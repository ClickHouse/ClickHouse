-- Tags: no-fasttest
-- no-fasttest: needs the native Parquet V3 reader.

-- Regression test for a silent wrong-results bug in the ParquetV3 native reader's
-- preparePrewhere(). When the prewhere splitter promotes a shared intermediate to a
-- step boundary, the later step references it by its generated result_name. If a real
-- physical column happens to share that generated name (here a column literally named
-- 'equals(length(a), 3_UInt8)'), a name-only fallback check would keep the split path
-- and feed the physical column to the later step instead of the computed intermediate.
-- The split path is only correct when every step input is an original prewhere input or
-- an intermediate produced into a dedicated prewhere-output slot; otherwise we fall back
-- to a single step.

SET engine_file_truncate_on_insert = 1;

-- The physical column 'equals(length(a), 3_UInt8)' is 0 in every row, while the true
-- value of the promoted intermediate equals(length('foo'), 3) is 1. With a=foo, b=5, c=7
-- the prewhere passes all 10 rows; if the physical column (0) leaked in, it would pass 0.
INSERT INTO FUNCTION file(currentDatabase() || '_04342.parquet', Parquet,
    'a String, b Int64, c Int64, `equals(length(a), 3_UInt8)` UInt8')
    SELECT 'foo', 5, 7, 0 FROM numbers(10);

-- Selecting the colliding column puts it into the reader's sample block; the two
-- and-groups share equals(length(a), 3) so the splitter promotes it to a step boundary.
SELECT count(), max(`equals(length(a), 3_UInt8)`)
FROM file(currentDatabase() || '_04342.parquet', Parquet,
    'a String, b Int64, c Int64, `equals(length(a), 3_UInt8)` UInt8')
PREWHERE (length(a) = 3 AND b > 0) AND (length(a) = 3 AND c > 0)
SETTINGS input_format_parquet_use_native_reader_v3 = 1, enable_analyzer = 1,
    query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1;
