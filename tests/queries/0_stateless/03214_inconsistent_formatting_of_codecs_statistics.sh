#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Ensure that these (possibly incorrect) queries can at least be parsed back after formatting.
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t MODIFY COLUMN c CODEC(in(1, 2))" | $CLICKHOUSE_FORMAT --oneline
$CLICKHOUSE_FORMAT --oneline --query "ALTER TABLE t MODIFY COLUMN c STATISTICS(plus(1, 2))" | $CLICKHOUSE_FORMAT --oneline
