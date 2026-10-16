-- Tags: no-fasttest
-- - no-fasttest: uses the s3 table function

-- Lazy materialization for a plain (mutable) object storage requires the second-pass read to be
-- pinned to the object generation the main pass saw. On S3 that pin is the captured ETag
-- (`If-Match` on the GET, see `ReadBufferFromS3`), but GCS accessed through the S3 API is
-- documented to legitimately return objects without an ETag, so a GCS-provider client must stay
-- on the single-pass plan even with `s3_validate_etag_on_read` enabled.
-- The provider is deduced from the endpoint at client creation and the plan is built without any
-- network I/O, so unresolvable `.invalid` endpoints are enough to test the plan-time gate.

SET enable_analyzer = 1;
SET s3_validate_etag_on_read = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 0;
SET query_plan_optimize_lazy_materialization_for_object_storage = 1;

SELECT '-- GCS provider: the reread cannot be proven ETag-pinned, no lazy step';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%')
FROM (EXPLAIN SELECT s FROM s3('https://storage.googleapis.com.invalid/bucket/data.parquet', NOSIGN, 'Parquet', 'k UInt64, s String') ORDER BY k LIMIT 3);

SELECT '-- AWS provider: the reread is ETag-pinned, the lazy step is present (non-vacuous control)';
SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%')
FROM (EXPLAIN SELECT s FROM s3('https://bucket.s3.amazonaws.com.invalid/data.parquet', NOSIGN, 'Parquet', 'k UInt64, s String') ORDER BY k LIMIT 3);
