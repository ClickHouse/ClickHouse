#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT((((((((((SELECT(((((((((SELECT((((((((((SELECT(((((((((SELECT((((((((((SELECT(((((((((SELECT 1+)))))))))))))))))))))))))))))))))))))))))))))))))))))))))" 2>&1 | grep -o -F 'TOO_SLOW_PARSING'
