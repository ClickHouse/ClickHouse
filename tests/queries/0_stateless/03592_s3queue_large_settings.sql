-- Tags: no-fasttest

CREATE TABLE s3_queue (name String, value UInt32) ENGINE = S3Queue('http://localhost:11111/test/{a,b,c}.tsv', 'user', 'password', CSV) SETTINGS s3queue_tracked_files_limit = 18446744073709551615, mode = 'ordered';

DETACH TABLE s3_queue;
ATTACH TABLE s3_queue;
