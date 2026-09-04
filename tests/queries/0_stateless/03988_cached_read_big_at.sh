#!/usr/bin/env bash
# Tags: stateful, no-parallel, no-random-settings, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"

# Reproduces issue from https://github.com/ClickHouse/ClickHouse/issues/97325
# --send_logs_level=fatal: the long read against the public AWS bucket can hit
# transient retryable 5xx that the S3 retry strategy recovers from.
# Use the default parallel download count so the large regression query stays below the
# 300-second test limit. The query still exercises the filesystem-cache `readBigAt` path.
$CLICKHOUSE_CLIENT --send_logs_level=fatal -q "
    SELECT
        (min(Title) <= max(Title)) AND
        (min(URL) <= max(URL)) AND
        (min(SearchPhrase) <= max(SearchPhrase))
    FROM test.hits_parquet
    SETTINGS
        filesystem_cache_name = 'cache_for_readbigat',
        enable_filesystem_cache = 1
"
