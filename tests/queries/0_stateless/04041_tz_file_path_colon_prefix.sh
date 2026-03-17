#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verify clickhouse-local works with POSIX TZ file path syntax (TZ=:/etc/localtime)
# See https://github.com/ClickHouse/ClickHouse/issues/86495

if [ ! -f /etc/localtime ]; then
    echo 1
    exit 0
fi

TZ=:/etc/localtime $CLICKHOUSE_LOCAL --query "SELECT 1"
