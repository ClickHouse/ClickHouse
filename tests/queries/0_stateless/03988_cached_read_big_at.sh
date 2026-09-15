#!/usr/bin/env bash
# Tags: stateful, no-parallel, no-random-settings, long
# Tag no-parallel: Clears the shared `cache_for_readbigat` cache and reads 30 million rows within the test time limit.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"

# Reproduces issue from https://github.com/ClickHouse/ClickHouse/issues/97325
# --send_logs_level=fatal: the long read against the public AWS bucket can hit
# transient retryable 5xx that the S3 retry strategy recovers from.
# The original `readBigAt` failure was in row group 56, ending at row 28,205,948.
# Read the first 30 million rows in order to cover it within the 300-second test limit.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT
        (min(Title) <= max(Title)) AND
        (min(URL) <= max(URL)) AND
        (min(SearchPhrase) <= max(SearchPhrase))
    FROM
    (
        SELECT Title, URL, SearchPhrase
        FROM test.hits_parquet
        LIMIT 30000000
    )
    SETTINGS
        filesystem_cache_name = 'cache_for_readbigat',
        enable_filesystem_cache = 1,
        input_format_parquet_preserve_order = 1
"
