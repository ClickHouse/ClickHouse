#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $CLICKHOUSE_TEST_UNIQUE_NAME.xml
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.*')" 2>&1 | grep -c "CANNOT_DETECT_FORMAT"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.xml

