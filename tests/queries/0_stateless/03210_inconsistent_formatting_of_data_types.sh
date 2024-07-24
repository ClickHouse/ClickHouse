#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE columns_with_multiple_streams MODIFY COLUMN field1 Nullable(tupleElement(x, 2), UInt8)" | $CLICKHOUSE_FORMAT --oneline
