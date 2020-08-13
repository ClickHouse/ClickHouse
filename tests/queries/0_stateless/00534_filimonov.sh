#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# Server should not crash on any function trash calls

cat $CURDIR/00534_filimonov.data | $CLICKHOUSE_CLIENT -n --ignore-error >/dev/null 2>&1

$CLICKHOUSE_CLIENT -q "SELECT 'Still alive'"
