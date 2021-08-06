#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Big quiery. Memory limit
${CLICKHOUSE_CLIENT} --max_memory_usage_in_client=1 --testmode --query "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(1000) -- { serverError 241; }"

# small quiery. In max untracked memory range
${CLICKHOUSE_CLIENT} --max_memory_usage_in_client=1 --testmode --query "SELECT * FROM system.numbers LIMIT 60000"

# Have anough memory (unlimited). Better not to set limit, because of adress sanitizer
${CLICKHOUSE_CLIENT} --testmode --query "SELECT arrayMap(x -> range(x), range(number)) FROM numbers(125)"
