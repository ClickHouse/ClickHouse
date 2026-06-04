-- Tags: no-fasttest
-- no-fasttest: requires the `s3_conn` named collection / S3 (MinIO), absent in fasttest.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106147
-- A compound `WHERE` with `IS NOT NULL` over an object-storage table (S3/Iceberg) used to throw
-- `NOT_FOUND_COLUMN_IN_BLOCK` with the Parquet V3 native reader. `optimize_functions_to_subcolumns`
-- rewrote `isNotNull(foo)` into reading the `foo.null` subcolumn, which the V3 `PREWHERE` path
-- cannot supply as a standalone input. `StorageObjectStorage` must disable that optimization
-- (like `StorageFile`/`StorageURL` do), so `IS NOT NULL` stays a function over the full column.

DROP TABLE IF EXISTS t_04303;

CREATE TABLE t_04303 (key String, foo Nullable(Decimal(38, 3)), bar String)
ENGINE = S3(s3_conn, filename = 'test_04303_isnotnull', format = Parquet);

INSERT INTO t_04303 SETTINGS s3_truncate_on_insert = 1
VALUES ('X', 1.5, 'baz'), ('Y', NULL, 'qux'), ('X', NULL, 'skip');

-- `foo` is referenced only in `WHERE`, not in `SELECT`: the buggy path moves the rewritten
-- `not(foo.null)` subcolumn into `PREWHERE` and fails to build the reader's filter.
-- All triggers are pinned so random-settings variants still reproduce the bug on unfixed builds:
--   * `enable_analyzer = 1` / `optimize_functions_to_subcolumns = 1`: the `isNotNull(foo)` ->
--     `not(foo.null)` rewrite runs only in the new analyzer;
--   * `query_plan_enable_optimizations = 1` & `query_plan_optimize_prewhere = 1`: the new-analyzer
--     `PREWHERE` push-down is gated on both (`optimize_prewhere = the two &&'d`);
--   * `input_format_parquet_use_native_reader_v3 = 1`: the V3 reader that cannot supply `foo.null`.
SELECT bar FROM t_04303
WHERE key = 'X' AND foo IS NOT NULL
SETTINGS input_format_parquet_use_native_reader_v3 = 1, optimize_move_to_prewhere = 1, optimize_functions_to_subcolumns = 1, enable_analyzer = 1, query_plan_enable_optimizations = 1, query_plan_optimize_prewhere = 1;

DROP TABLE t_04303;
