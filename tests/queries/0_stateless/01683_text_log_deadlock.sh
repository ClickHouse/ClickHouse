#!/usr/bin/env bash
# Tags: deadlock, no-parallel
# Tag no-parallel: reproduces a deadlock in the server-global text_log sink under many concurrent connections; if it hangs, it blocks the shared server for every other running test, not just this one

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK -i 5000 -c 32 --query 'SELECT 1' |& grep -oF 'queries: 5000'
