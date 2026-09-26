#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "CREATE TABLE mt (a UInt64, projection p (select * order by a)) ENGINE = MergeTree ORDER BY a;"
$CLICKHOUSE_LOCAL -q "SELECT 'OK'"
