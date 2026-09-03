#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(NULL)" 2>&1 | grep -q "ILLEGAL_COLUMN" && echo "OK" || echo "FAIL"
$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(materialize(NULL))" 2>&1 | grep -q "ILLEGAL_TYPE_OF_ARGUMENT" && echo "OK" || echo "FAIL"
$CLICKHOUSE_LOCAL -q "SELECT assumeNotNull(materialize(NULL)) from numbers(10)" 2>&1 | grep -q "ILLEGAL_TYPE_OF_ARGUMENT" && echo "OK" || echo "FAIL"

