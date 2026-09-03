#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


echo '[{"number":"0"} ,{"number":"1"} , {"number":"2"}]' > $CLICKHOUSE_TMP/02182_data
$CLICKHOUSE_LOCAL -q "SELECT * FROM table" --file $CLICKHOUSE_TMP/02182_data --input-format JSONEachRow

echo '["0"] ,["1"] ; ["2"]' > $CLICKHOUSE_TMP/02182_data
$CLICKHOUSE_LOCAL -q "SELECT * FROM table" --file $CLICKHOUSE_TMP/02182_data --input-format JSONCompactEachRow

