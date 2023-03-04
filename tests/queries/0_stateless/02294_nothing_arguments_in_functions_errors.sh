#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(NULL)" 2>&1 | grep -q "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"
$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(materialize(NULL)) SETTINGS allow_experimental_analyzer = 1" 2>&1 | grep -q "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"
$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(materialize(NULL)) from numbers(10) SETTINGS allow_experimental_analyzer = 1" 2>&1 | grep -q "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"

