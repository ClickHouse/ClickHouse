#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1,2" > $CLICKHOUSE_TEST_UNIQUE_NAME.csv
sleep 1
$CLICKHOUSE_LOCAL -m -q "
select _size, (dateDiff('millisecond', _time, now()) < 600000 AND dateDiff('millisecond', _time, now()) > 0) from file('$CLICKHOUSE_TEST_UNIQUE_NAME.csv');
"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.csv
