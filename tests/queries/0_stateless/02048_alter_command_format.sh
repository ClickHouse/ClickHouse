#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "alter table t drop column a, drop column b, drop column c, add column d UInt8" | $CLICKHOUSE_FORMAT;
