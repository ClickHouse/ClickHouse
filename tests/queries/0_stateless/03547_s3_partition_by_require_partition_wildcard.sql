-- Tags: no-parallel, no-fasttest, no-random-settings

CREATE TABLE s3_03547 (id UInt64) ENGINE=S3(s3_conn, format=Parquet) PARTITION BY id; -- {serverError BAD_ARGUMENTS}
