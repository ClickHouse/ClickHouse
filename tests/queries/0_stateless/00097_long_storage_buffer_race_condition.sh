#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
[ "$NO_SHELL_CONFIG" ] || . "$CURDIR"/../shell_config.sh

seq 1 1000 | sed -r 's/.+/CREATE TABLE IF NOT EXISTS buf_00097 (a UInt8) ENGINE = Buffer('$CLICKHOUSE_DATABASE', b, 1, 1, 1, 1, 1, 1, 1); DROP TABLE buf_00097;/' | $CLICKHOUSE_CLIENT -n
