#!/usr/bin/env bash
# Tags: no-parallel, long

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
$CLICKHOUSE_CLIENT --max_memory_usage 16G -m -q "
    INSERT INTO FUNCTION s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/test_INT_MAX.tsv', '', '', 'TSV')
    SELECT repeat('a', 1024) FROM numbers((pow(2, 30) * 2) / 1024)
    SETTINGS s3_max_single_part_upload_size = '5Gi';

    SELECT count() FROM s3('http://localhost:11111/test/$CLICKHOUSE_DATABASE/test_INT_MAX.tsv');
"
