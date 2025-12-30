-- Tags: no-fasttest

CREATE TABLE s3queue_test
(
    `column1` UInt32,
    `column2` UInt32,
    `column3` UInt32
)
ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/s3queue_test_data/', 'username', 'password', CSV)
SETTINGS s3queue_loading_retries = 0, after_processing = 'delete', keeper_path = '/s3queue', mode = 'ordered', enable_hash_ring_filtering = 1, s3queue_enable_logging_to_s3queue_log = 1;

SHOW CREATE TABLE s3queue_test;
