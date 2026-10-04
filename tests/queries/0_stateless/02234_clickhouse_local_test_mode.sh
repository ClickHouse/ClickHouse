#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query="SELECT n SETTINGS enable_analyzer = 1" 2>&1 | grep -q "Code: 47. DB::Exception:" && echo 'OK' || echo 'FAIL'
$CLICKHOUSE_LOCAL --query="SELECT n SETTINGS enable_analyzer = 0" 2>&1 | grep -q "Code: 47. DB::Exception:" && echo 'OK' || echo 'FAIL'
