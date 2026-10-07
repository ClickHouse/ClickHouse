#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --external --scalar --name foo --format CSV --file <(echo one-scalar) --types 'String' -q "SELECT __getScalar('foo')"
$CLICKHOUSE_CLIENT --external --scalar --name foo --format CSV --file <(echo two-columns,scalar) --types 'String, String' -q "SELECT __getScalar('foo')"
$CLICKHOUSE_CLIENT --external --scalar --name foo --format CSV --file <(echo two-columns,scalar) --structure 'a String, b String' -q "SELECT __getScalar('foo').b"
