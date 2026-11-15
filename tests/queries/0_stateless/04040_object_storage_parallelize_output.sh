#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that reading a single file from object storage (ENGINE = S3)
# parallelizes the pipeline output via Resize, so downstream processors
# like AggregatingTransform can run on multiple threads.
# Without this, queries on a single S3/data-lake Parquet file run
# entirely single-threaded (e.g. Q28 in ClickBench: 79× slower).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE test_s3_${CLICKHOUSE_DATABASE} (x UInt64, y String)
    ENGINE = S3('http://localhost:19999/dummy.parquet', Parquet);
"

# The pipeline should contain 'Resize 1 → 4' between the source and
# the processing transforms, proving the single source output is
# distributed across max_threads workers.
${CLICKHOUSE_CLIENT} --query "
    EXPLAIN PIPELINE
    SELECT count(), sum(x) FROM test_s3_${CLICKHOUSE_DATABASE}
    GROUP BY y
    SETTINGS max_threads = 4;
" | grep -o 'Resize 1 → 4'

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_s3_${CLICKHOUSE_DATABASE};"
