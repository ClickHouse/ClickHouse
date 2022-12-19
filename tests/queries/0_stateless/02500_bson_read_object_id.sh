#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "desc file('data_bson/comments.bson')"
$CLICKHOUSE_LOCAL -q "select _id from file('data_bson/comments.bson') format Null"

