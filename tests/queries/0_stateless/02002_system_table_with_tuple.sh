#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.tables \
    WHERE database IN ('system', '$CLICKHOUSE_DATABASE') \
    SETTINGS legacy_column_name_of_tuple_literal = 1"
