#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

clickhouse_client_removed_host_parameter --host="${CLICKHOUSE_HOST}" --query="SELECT * FROM ext" --format=Vertical --external --file=- --structure="s String" --name=ext --format=JSONEachRow <<< '{"s":"Hello"}'
