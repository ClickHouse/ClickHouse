-- Tags: no-fasttest
-- Tag no-fasttest: uses the S3 mock server on localhost:11111

INSERT INTO FUNCTION s3('http://localhost:11111/test/04104_url_time.csv', 'test', 'testtest', 'CSV', 'a UInt32')
SETTINGS s3_truncate_on_insert = 1
SELECT number FROM numbers(2);

-- MinIO sets Last-Modified, so _time must be populated.
SELECT _time IS NOT NULL AND _time > toDateTime('2020-01-01 00:00:00')
FROM url('http://localhost:11111/test/04104_url_time.csv', 'CSV', 'a UInt32')
LIMIT 1;

-- All rows in one file share the same _time.
SELECT count(DISTINCT _time)
FROM url('http://localhost:11111/test/04104_url_time.csv', 'CSV', 'a UInt32');

-- A response without a Last-Modified header must yield NULL.
-- ClickHouse's own HTTP query endpoint does not set Last-Modified.
SELECT _time
FROM url('http://127.0.0.1:8123/?query=select+1&user=default', LineAsString, 's String');

-- urlCluster goes through StorageURLSource on each worker; _time must be populated there too.
SELECT _time IS NOT NULL AND _time > toDateTime('2020-01-01 00:00:00')
FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/04104_url_time.csv', 'CSV', 'a UInt32')
LIMIT 1;

SELECT count(DISTINCT _time)
FROM urlCluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/04104_url_time.csv', 'CSV', 'a UInt32');
