#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SYSTEM JEMALLOC FLUSH PROFILE" -- --enable_jemalloc_global_profiler | grep -q "/tmp/jemalloc_clickhouse" || echo "Didn't return profile path"