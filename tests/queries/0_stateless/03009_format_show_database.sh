#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "show database default"
$CLICKHOUSE_LOCAL -q "show database default2" 2>&1 | grep -o 'UNKNOWN_DATABASE'
