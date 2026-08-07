#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local and clickhouse-client behave the same
$CLICKHOUSE_CLIENT -q "select 1; select 2;"
$CLICKHOUSE_LOCAL -q "select 1; select 2;"

# -n is a no-op
$CLICKHOUSE_CLIENT -q "select 1; select 2;"
$CLICKHOUSE_LOCAL -q "select 1; select 2;"

exit 0
