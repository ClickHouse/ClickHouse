#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS map_formats_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE map_formats_input (seq UInt32, orders Map(UInt32), prices Map(Float32), phones Map(String), birthday Map(Date)) ENGINE = Memory;" --allow_experimental_map_type 1

echo "JSONEachRow"
$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT JSONEachRow" <<< '{"seq":1,"orders":{"apple":1,"banana":2},"prices":{"apple":8.4,"mango":5.2},"phones":{"fire":"119","medical":"120"},"birthday":{"Jay":"2000-01-01"}}'
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

echo "CSV"
$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT CSV" <<< "1,\"{'apple':1,'banana':2}\",\"{'apple':8.4,'mango':5.2}\",\"{'fire':'119','medical':'120'}\",\"{'Jay':'2000-01-01'}\""
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE map_formats_input"

echo "TSV"
$CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT TSV" <<< "1	{'apple':1,'banana':2}	{'apple':8.4,'mango':5.2}	{'fire':'119','medical':'120'}	{'Jay':'2000-01-01'}"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

echo "Native"
$CLICKHOUSE_CLIENT -q 'SELECT * FROM map_formats_input FORMAT Native' | $CLICKHOUSE_CLIENT -q "INSERT INTO map_formats_input FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT * FROM map_formats_input"

$CLICKHOUSE_CLIENT -q "DROP TABLE map_formats_input"
