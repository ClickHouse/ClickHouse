#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: It uses system.errors values which are global

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Simply calling a table function correctly should not increase system.errors
OLD_ERROR_QUANTITY=$(${CLICKHOUSE_CLIENT} --query "SELECT sum(value) FROM system.errors WHERE name = 'UNKNOWN_TABLE'")
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM numbers(10)"
${CLICKHOUSE_CLIENT} --query "SELECT sum(value) = ${OLD_ERROR_QUANTITY}  FROM system.errors WHERE name = 'UNKNOWN_TABLE'"
