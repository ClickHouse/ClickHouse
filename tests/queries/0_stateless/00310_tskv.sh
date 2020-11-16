#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS tskv";
$CLICKHOUSE_CLIENT --query="CREATE TABLE tskv (tskv_format String, timestamp DateTime('UTC'), timezone String, text String, binary_data String) ENGINE = Memory";

# shellcheck disable=SC2028
echo -n 'tskv	tskv_format=custom-service-log	timestamp=2013-01-01 00:00:00	timezone=+0400	text=multiline\ntext	binary_data=can contain \0 symbol
binary_data=abc	text=Hello, world
binary_data=def	text=
tskv

' | $CLICKHOUSE_CLIENT --query="INSERT INTO tskv FORMAT TSKV";

$CLICKHOUSE_CLIENT --query="SELECT * FROM tskv ORDER BY binary_data";
$CLICKHOUSE_CLIENT --query="DROP TABLE tskv";
