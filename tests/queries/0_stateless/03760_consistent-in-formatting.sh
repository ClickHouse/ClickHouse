#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CLICKHOUSE_FORMAT" --query --oneline 'SELECT position(1 = ANY(SELECT 1), 1)'
"$CLICKHOUSE_FORMAT" --query --oneline 'SELECT position((1 IN (SELECT 1)), 1)'
"$CLICKHOUSE_FORMAT" --query --oneline 'SELECT position((1 NOT IN (SELECT 1)), 1)'
