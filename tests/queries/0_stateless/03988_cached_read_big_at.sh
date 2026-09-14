#!/usr/bin/env bash
# Tags: stateful, no-parallel, no-random-settings, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"

# Reproduces issue from https://github.com/ClickHouse/ClickHouse/issues/97325
$CLICKHOUSE_CLIENT -q "
    SELECT
        (min(Title) <= max(Title)) AND
        (min(URL) <= max(URL)) AND
        (min(SearchPhrase) <= max(SearchPhrase))
    FROM test.hits_parquet
    SETTINGS
        filesystem_cache_name = 'cache_for_readbigat',
        enable_filesystem_cache = 1,
        max_download_threads = 1
"
