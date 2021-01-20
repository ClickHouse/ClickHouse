#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS lv"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS mt"
$CLICKHOUSE_CLIENT --query="CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple()"
$CLICKHOUSE_CLIENT --allow_experimental_live_view 1 --query="CREATE LIVE VIEW lv AS SELECT * FROM mt"
$CLICKHOUSE_CLIENT --query="INSERT INTO mt VALUES (1),(2),(3)"
$CLICKHOUSE_CLIENT --allow_experimental_live_view 1 --query="SELECT * FROM lv FORMAT JSONEachRowWithProgress" \
    | sed -r -e 's/"elapsed_time":"[0-9]+"/"elapsed_time":"<ELAPSED-TIME>"/' \
    | awk '/"progress"/{ progresstail += $0 } /"row"/{ print $0 } END{ print $progresstail }'
$CLICKHOUSE_CLIENT --query="DROP TABLE lv"
$CLICKHOUSE_CLIENT --query="DROP TABLE mt"
