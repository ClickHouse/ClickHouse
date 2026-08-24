#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL -q "select toUInt32(number) as x from numbers(10) format JSONEachRow" > data.jsoneachrow

$CLICKHOUSE_LOCAL -q "desc table table" < data.jsoneachrow
$CLICKHOUSE_LOCAL -q "select * from table" < data.jsoneachrow

rm data.jsoneachrow

echo -e "1\t2\t3" | $CLICKHOUSE_LOCAL -q "desc table table" --file=-
echo -e "1\t2\t3" | $CLICKHOUSE_LOCAL -q "select * from table" --file=-

