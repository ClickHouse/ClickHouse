#!/usr/bin/env bash
# Tags: no-parallel, long, no-fasttest, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage
# Tag no-parallel: deliberately buffers a ~2 GiB S3 part in memory (`max_memory_usage 16G`,
# up to 300s execution time) to reproduce a part-size-exceeds-INT_MAX regression; running
# concurrently with other tests risks OOM-ing or timing out the shared test server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for crash in case of part exceeds INT_MAX
#
# NOTE: .sh test is used over .sql because it needs $CLICKHOUSE_DATABASE to
# avoid truncation, since seems that the version of MinIO that is used on CI
# too slow with this.
#
# Unfortunately, the test has to buffer it in memory.
$CLICKHOUSE_CLIENT --max_execution_time 300 --max_memory_usage 16G -m -q "
    INSERT INTO FUNCTION s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/test_INT_MAX.tsv', '', '', 'TSV')
    SELECT repeat('a', 1024) FROM numbers((pow(2, 30) * 2) / 1024)
    SETTINGS s3_max_single_part_upload_size = '5Gi', s3_retry_attempts=5;

    SELECT count() FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/test_INT_MAX.tsv')
    SETTINGS s3_retry_attempts=5;
"
