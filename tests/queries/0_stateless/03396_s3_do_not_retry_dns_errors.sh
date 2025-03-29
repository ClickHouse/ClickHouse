#!/usr/bin/env bash
# Tags: no-fasttest

# Test from https://github.com/ClickHouse/ClickHouse/issues/68663

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    CREATE TABLE IF NOT EXISTS test_s3_issue_queue
    (
        test_field String
    )
    ENGINE = S3Queue('http://minio.such-tld-will-never-be-registered-for-clickhose-tests:9000/not-exist-s3-bucket/**.json', JSONEachRow)
    SETTINGS keeper_path = '/clickhouse/db_name/s3queue/test_s3_issue', mode = 'unordered', after_processing = 'keep', s3queue_loading_retries = 3, s3queue_processing_threads_num = 30, s3queue_enable_logging_to_s3queue_log = 1, s3queue_polling_min_timeout_ms = 1000, s3queue_polling_max_timeout_ms = 5000, s3queue_polling_backoff_ms = 1500, s3queue_tracked_file_ttl_sec = 345600, s3queue_tracked_files_limit = 1000000;

    CREATE TABLE IF NOT EXISTS test_s3_issue (test_field String) ENGINE = Null;
    CREATE MATERIALIZED VIEW IF NOT EXISTS test_s3_issue_mv TO test_s3_issue AS SELECT * FROM test_s3_issue_queue;
" |& grep -v "AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider: Token file must be specified to use STS AssumeRole web identity creds provider."

# just to ensure that some streaming will be started, the test cannot be flaky
# because of this, it can simply does not check what it should properly, but
# overcomplicating the test does not worth it
sleep 5

# 20 seconds should be enough to ensure that DNS_ERROR will not be retried 100
# times, but simply only few
timeout 20s $CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS test_s3_issue_mv SYNC;
"
