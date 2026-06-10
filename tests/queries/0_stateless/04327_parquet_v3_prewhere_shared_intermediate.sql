-- Tags: no-fasttest
-- no-fasttest: needs the native Parquet V3 reader.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106147
-- A PREWHERE whose conjuncts share an intermediate expression (here the common
-- subexpression equals(a, 'foo') used by both multiIf columns) used to throw
-- NOT_FOUND_COLUMN_IN_BLOCK with the Parquet V3 reader, because the prewhere
-- splitter promoted that intermediate to a step boundary the reader cannot address.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04327.parquet', Parquet)
    SELECT 'foo' AS a, 'baz' AS b, 100 AS ts;

-- The window/outer wrapper makes the optimizer build a runtime join filter and push
-- both it and isNotNull(m2) into PREWHERE; both depend on the shared equals(a, 'foo').
-- All triggers are pinned so random-settings variants still reproduce on unfixed builds.
WITH flat AS
(
    SELECT
        multiIf(a = 'foo', 'bar', NULL) AS m1,
        multiIf(a = 'foo', b, NULL) AS m2,
        ts
    FROM file(currentDatabase() || '_04327.parquet', Parquet, 'a Nullable(String), b Nullable(String), ts Nullable(Int64)')
)
SELECT m2, ts, rn
FROM
(
    SELECT
        f.m2,
        f.ts,
        row_number() OVER (PARTITION BY 1 ORDER BY f.ts DESC) AS rn
    FROM flat f
    INNER JOIN (SELECT 'bar' AS k) p ON f.m1 = p.k
    WHERE f.m2 IS NOT NULL
)
WHERE rn = 1
SETTINGS input_format_parquet_use_native_reader_v3 = 1, optimize_move_to_prewhere = 1, enable_analyzer = 1, query_plan_enable_optimizations = 1, query_plan_optimize_prewhere = 1;
