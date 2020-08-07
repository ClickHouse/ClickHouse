#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --param_x '\N' --query 'SELECT {x:Nullable(Nothing)} as a' --format TSVWithNamesAndTypes
