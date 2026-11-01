-- Tags: no-fasttest, no-replicated-database, no-distributed-cache
-- ^ no-fasttest: needs MinIO + s3 endpoints from CI fixtures
--   no-replicated-database: stateless test using `disk = '...'`

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/89300
-- Calling a data-lake table function with `SETTINGS disk = '<disk>'` used to throw
-- a bad-cast assertion (from the wrapping decorator down to `S3ObjectStorage`)
-- whenever the disk wrapped its underlying object storage (e.g. cache layer).
-- The same shape of bug previously affected `PlainObjectStorage` over `S3ObjectStorage`
-- before that decorator was removed in #90580.
--
-- The fix unwraps decorator object storages via `IObjectStorage::getUnderlying`
-- before casting to the concrete `S3ObjectStorage` in `S3StorageParsedArguments::fromDisk`.
--
-- We do not exercise reading any real iceberg data here; the assertion is purely
-- that argument parsing does not abort. The query itself is expected to fail at
-- table-resolution time with an S3/iceberg error code.
--
-- NOTE: this comment intentionally avoids quoting the exception text verbatim,
-- because the stress-test log scanner greps for that pattern in the server log
-- and a literal copy in the query log produces false-positive failure reports
-- (the SQL comment is echoed into the `<Error> executeQuery` entry on failure).

-- Cached disk (`CachedObjectStorage` wrapping `S3ObjectStorage`) — the bug
-- originally reported from a BuzzHouse fuzzer hit on PR #94148.
SELECT 1 FROM icebergS3('no_such_table_for_89300_cache', 'Parquet', 'a Int',
                        SETTINGS disk = 's3_cache')
; -- { serverError S3_ERROR, FILE_DOESNT_EXIST, BAD_REQUEST_PARAMETER, ICEBERG_SPECIFICATION_VIOLATION, RESOURCE_NOT_FOUND, FORMAT_IS_NOT_SUITABLE_FOR_INPUT, CANNOT_PARSE_INPUT_ASSERTION_FAILED, CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Plain-metadata S3 disk — historically a `PlainObjectStorage<S3ObjectStorage>` wrapper
-- (the original symptom in #89300). The decorator class itself was removed in #90580
-- so the cast now succeeds even without this PR's change, but we keep this case as a
-- guard against any reintroduction of similar wrapping for plain disks.
SELECT 1 FROM icebergS3('no_such_table_for_89300_plain', 'Parquet', 'a Int',
                        SETTINGS disk = 's3_plain_disk')
; -- { serverError S3_ERROR, FILE_DOESNT_EXIST, BAD_REQUEST_PARAMETER, ICEBERG_SPECIFICATION_VIOLATION, RESOURCE_NOT_FOUND, FORMAT_IS_NOT_SUITABLE_FOR_INPUT, CANNOT_PARSE_INPUT_ASSERTION_FAILED, CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Sanity check: the same query on a raw S3 disk (no decorator) keeps working.
SELECT 1 FROM icebergS3('no_such_table_for_89300_raw', 'Parquet', 'a Int',
                        SETTINGS disk = 's3_disk')
; -- { serverError S3_ERROR, FILE_DOESNT_EXIST, BAD_REQUEST_PARAMETER, ICEBERG_SPECIFICATION_VIOLATION, RESOURCE_NOT_FOUND, FORMAT_IS_NOT_SUITABLE_FOR_INPUT, CANNOT_PARSE_INPUT_ASSERTION_FAILED, CANNOT_EXTRACT_TABLE_STRUCTURE }
