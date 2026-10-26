-- Tags: no-fasttest, no-random-settings
-- Tag no-fasttest: Depends on S3

-- When no explicit `partition_strategy` is given, a `{_partition_id}` placeholder in the
-- path implies the `wildcard` strategy regardless of the
-- `file_like_engine_default_partition_strategy` default, because `hive` cannot work with
-- such a path anyway. This keeps pre-26.6 DDL working under the 26.6 `hive` default.

-- All S3 keys are prefixed with `currentDatabase()` so that parallel and repeated runs
-- of this test do not see each other's objects.

SET file_like_engine_default_partition_strategy = 'hive';

CREATE TABLE test_04614_implicit_wildcard (a UInt64, b String)
ENGINE = S3(s3_conn, filename = currentDatabase() || '/tbl_{_partition_id}', format = Parquet)
PARTITION BY a;

SET s3_truncate_on_insert = 1;
INSERT INTO test_04614_implicit_wildcard VALUES (1, 'a'), (22, 'b'), (333, 'c');
SELECT a, b FROM s3(s3_conn, filename = currentDatabase() || '/tbl_*', format = Parquet) ORDER BY a;

-- The table must survive DETACH / ATTACH with the `hive` default still in effect.
DETACH TABLE test_04614_implicit_wildcard;
ATTACH TABLE test_04614_implicit_wildcard;
INSERT INTO test_04614_implicit_wildcard VALUES (4444, 'd');
SELECT a, b FROM s3(s3_conn, filename = currentDatabase() || '/tbl_*', format = Parquet) ORDER BY a;

-- The same path with an explicit `partition_strategy = 'hive'` must still be rejected.
CREATE TABLE test_04614_explicit_hive (a UInt64, b String)
ENGINE = S3(s3_conn, filename = currentDatabase() || '/hive_{_partition_id}', format = Parquet, partition_strategy = 'hive')
PARTITION BY a; -- {serverError BAD_ARGUMENTS}

-- The implicit wildcard also applies to table functions (INSERT INTO FUNCTION ... PARTITION BY).
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/fn_{_partition_id}', format = Parquet)
PARTITION BY a SELECT 55::UInt64 AS a, 'e' AS b;
SELECT a, b FROM s3(s3_conn, filename = currentDatabase() || '/fn_*', format = Parquet) ORDER BY a;

DROP TABLE test_04614_implicit_wildcard;
