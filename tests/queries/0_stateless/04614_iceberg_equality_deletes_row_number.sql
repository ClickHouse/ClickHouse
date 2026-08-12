-- Tags: no-fasttest, no-parallel-replicas
-- - no-fasttest: reads Iceberg tables from Minio

-- Regression for the `_row_number` virtual column on Iceberg tables with equality deletes.
-- Equality deletes used to be applied by a plain `FilterTransform`, which does not maintain
-- `ChunkInfoRowNumbers`, so `_row_number` produced wrong values (or an exception). They now use
-- `RowNumbersPreservingFilterTransform`, so `_row_number` stays a correct physical row number.

-- Selecting `_row_number` must not throw and must not change the data.
SELECT sum(id) FROM (SELECT id, _row_number FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table'));

-- Every surviving row has a known physical row number (not NULL).
SELECT countIf(_row_number IS NULL) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- After equality deletes, each surviving row keeps a distinct (file, physical row number) identity.
SELECT count() = uniqExact((_file, _row_number)) FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table');

-- Surviving rows keep their physical row numbers, with gaps at the deleted positions.
-- The rows with `id < 10` are the second batch of the table: two data files with `id` 0..4
-- and 5..9 at physical rows 0..4 of each file. An equality delete removes `id = 3` and
-- `id = 9`, so `id = 4` must keep the physical row number 4 (not get renumbered to 3).
SELECT id, _row_number FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table') WHERE id < 10 ORDER BY id;

-- Equality deletes force reading all physical columns of the data files they apply to,
-- so this table must not take the LIMIT lazy materialization path even when it is enabled.
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (
    EXPLAIN SELECT name FROM icebergS3(s3_conn, filename = 'deletes_db/eq_deletes_table') ORDER BY id LIMIT 3
)
SETTINGS enable_analyzer = 1, -- lazy materialization requires the analyzer, so make this check non-vacuous
         query_plan_optimize_lazy_materialization = 1,
         query_plan_max_limit_for_lazy_materialization = 0,
         query_plan_optimize_lazy_materialization_for_object_storage = 1;
