#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --host=localhost --query="SELECT 1";
$CLICKHOUSE_CLIENT --host localhost --query "SELECT 1";
$CLICKHOUSE_CLIENT -hlocalhost -q"SELECT 1";
