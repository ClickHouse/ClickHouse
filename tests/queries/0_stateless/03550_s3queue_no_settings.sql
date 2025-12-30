-- Tags: no-fasttest

CREATE TABLE s3_queue (name String, value UInt32) ENGINE = S3Queue('http://localhost:11111/test/{a,b,c}.tsv'); -- { serverError BAD_ARGUMENTS }
