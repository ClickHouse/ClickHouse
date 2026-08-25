#!/usr/bin/env bash

CLICKHOUSE_DATABASE=no_such_database_could_exist

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT 1" |& grep -q UNKNOWN_DATABASE
