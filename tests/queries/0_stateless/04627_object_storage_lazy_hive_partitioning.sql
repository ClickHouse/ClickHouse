-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- CREATE and ATTACH of an object storage table with an explicit schema and format must not access
-- the endpoint. The hive partitioning sample path is resolved lazily on the first use of the table.

DROP TABLE IF EXISTS 04627_unreachable, 04627_hive, 04627_wrong_creds;

CREATE TABLE 04627_unreachable (id UInt64, val String)
ENGINE = S3('http://localhost:1/no-such-bucket/*.parquet', 'test', 'testtest', 'Parquet');

DETACH TABLE 04627_unreachable;
ATTACH TABLE 04627_unreachable;

DROP TABLE 04627_unreachable;

-- Hive partition columns are still detected, on the first query over the table.
SET s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04627_hive/key=A/data.parquet', format = Parquet) SELECT 1 AS id;
INSERT INTO FUNCTION s3(s3_conn, url = 'http://localhost:11111/test/04627_hive/key=B/data.parquet', format = Parquet) SELECT 2 AS id;

CREATE TABLE 04627_hive (id UInt64)
ENGINE = S3(s3_conn, url = 'http://localhost:11111/test/04627_hive/**.parquet', format = Parquet);

-- The resolution follows the construction context, the triggering query settings do not override it.
SELECT id FROM 04627_hive ORDER BY id SETTINGS use_hive_partitioning = 0;

SELECT id, key FROM 04627_hive ORDER BY id;

DROP TABLE 04627_hive;

-- Wrong credentials make the resolution fail fast. By default the triggering query fails,
-- without `throw_on_hive_partitioning_resolution_failure` it runs with only a warning.
CREATE TABLE 04627_wrong_creds (id UInt64)
ENGINE = S3('http://localhost:11111/test/04627_hive/**.parquet', 'invalid', 'invalid', 'Parquet');

DESCRIBE TABLE 04627_wrong_creds; -- {serverError S3_ERROR}
DESCRIBE TABLE 04627_wrong_creds SETTINGS throw_on_hive_partitioning_resolution_failure = 0 FORMAT Null;

DROP TABLE 04627_wrong_creds;
