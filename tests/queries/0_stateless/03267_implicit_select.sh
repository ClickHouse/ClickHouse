#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL "1 + 2"
$CLICKHOUSE_LOCAL -q "1 + 2"
$CLICKHOUSE_LOCAL --query "1 + 2"
$CLICKHOUSE_LOCAL --implicit_select 0 --query "1 + 2" 2>&1 | grep -oF 'Syntax error'
$CLICKHOUSE_LOCAL --implicit_select 0 --query "SELECT 1 + 2"
