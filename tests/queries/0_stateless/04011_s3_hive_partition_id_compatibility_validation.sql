-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on AWS
-- Regression test for issue #106465:
-- When `file_like_engine_default_partition_strategy = 'hive'` is set implicitly via
-- `compatibility = '26.6'`, a path containing the `{_partition_id}` wildcard was silently
-- accepted instead of raising BAD_ARGUMENTS. The explicit `partition_strategy = 'hive'`
-- case correctly rejected such paths; the implicit case did not.

DROP TABLE IF EXISTS t_106465_explicit;
DROP TABLE IF EXISTS t_106465_implicit;
DROP TABLE IF EXISTS t_106465_valid_hive;
DROP TABLE IF EXISTS t_106465_wildcard_ok;

-- Test 1: Explicit partition_strategy='hive' + {_partition_id} path → must raise BAD_ARGUMENTS.
-- This is pre-existing correct behaviour; we verify it is not broken.
CREATE TABLE t_106465_explicit (d Date, x UInt64)
ENGINE = S3(s3_conn, filename = 'test_106465/explicit_{_partition_id}.parquet', format = Parquet, partition_strategy = 'hive')
PARTITION BY d; -- { serverError BAD_ARGUMENTS }

-- Test 2: Implicit hive via file_like_engine_default_partition_strategy = 'hive'
-- + {_partition_id} path → must also raise BAD_ARGUMENTS (the regression fix).
SET file_like_engine_default_partition_strategy = 'hive';
CREATE TABLE t_106465_implicit (d Date, x UInt64)
ENGINE = S3(s3_conn, filename = 'test_106465/implicit_{_partition_id}.parquet', format = Parquet)
PARTITION BY d; -- { serverError BAD_ARGUMENTS }
SET file_like_engine_default_partition_strategy = 'wildcard'; -- restore default

-- Test 3: Implicit hive via compatibility = '26.6' + {_partition_id} path
-- → must also raise BAD_ARGUMENTS (the original issue repro).
SET compatibility = '26.6';
CREATE TABLE t_106465_compat (d Date, x UInt64)
ENGINE = S3(s3_conn, filename = 'test_106465/compat_{_partition_id}.parquet', format = Parquet)
PARTITION BY d; -- { serverError BAD_ARGUMENTS }
SET compatibility = ''; -- restore default

-- Test 4: Implicit hive via file_like_engine_default_partition_strategy = 'hive'
-- + valid hive path (no {_partition_id}) → must succeed.
SET file_like_engine_default_partition_strategy = 'hive';
CREATE TABLE t_106465_valid_hive (d Date, x UInt64)
ENGINE = S3(s3_conn, filename = 'test_106465/valid_hive/', format = Parquet)
PARTITION BY d;
DROP TABLE t_106465_valid_hive;
SET file_like_engine_default_partition_strategy = 'wildcard'; -- restore default

-- Test 5: Default wildcard strategy + {_partition_id} path → must succeed (no regression).
CREATE TABLE t_106465_wildcard_ok (d Date, x UInt64)
ENGINE = S3(s3_conn, filename = 'test_106465/wildcard_{_partition_id}.parquet', format = Parquet)
PARTITION BY d;
DROP TABLE t_106465_wildcard_ok;
