#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS map_formats_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE map_formats_input (m Map(String, UInt32), m1 Map(String, Date), m2 Map(String, Array(UInt32))) ENGINE = Log;"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT JSONEachRow" <<< '{"m":{"k1":1,"k2":2,"k3":3},"m1":{"k1":"2020-05-05"},"m2":{"k1":[],"k2":[7,8]}}'
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT CSV" <<< "\"{'k1':1,'k2':2,'k3':3}\",\"{'k1':'2020-05-05'}\",\"{'k1':[],'k2':[7,8]}\""
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT TSV" <<< "{'k1':1,'k2':2,'k3':3}	{'k1':'2020-05-05'}	{'k1':[],'k2':[7,8]}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q 'SELECT * FROM map_formats_input FORMAT Native' | $CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q "DROP TABLE map_formats_input"
