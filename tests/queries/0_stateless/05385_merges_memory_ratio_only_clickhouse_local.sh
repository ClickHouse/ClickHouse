#!/usr/bin/env bash
# `clickhouse-local` must enable the merges/mutations memory gate when `merges_mutations_memory_usage_soft_limit`
# is left at 0 and only `merges_mutations_memory_usage_to_ram_ratio` is set explicitly: the limit is derived
# from the ratio, the same way the server does. Without either setting the gate stays disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function merges_limit_lines()
{
    $CLICKHOUSE_LOCAL --log-level=information --logger.console=1 -q "SELECT 1 FORMAT Null" "$@" 2>&1 \
        | grep -c "Merges and mutations memory limit is set to"
}

echo "ratio only: $(merges_limit_lines -- --merges_mutations_memory_usage_soft_limit=0 --merges_mutations_memory_usage_to_ram_ratio=0.5)"
echo "nothing set: $(merges_limit_lines)"
