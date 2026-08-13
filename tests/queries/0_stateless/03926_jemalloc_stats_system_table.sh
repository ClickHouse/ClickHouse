#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug
#       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# NOTE: jemalloc is disabled under sanitizers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check that system.jemalloc_stats returns data
echo "Testing jemalloc_stats table:"
${CLICKHOUSE_CLIENT} -q "SELECT length(stats) > 0 FROM system.jemalloc_stats";
