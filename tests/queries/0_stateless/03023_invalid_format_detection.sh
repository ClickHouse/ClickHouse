#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

touch $CLICKHOUSE_TEST_UNIQUE_NAME.unknown
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.u*')" 2>&1 | grep -c "CANNOT_DETECT_FORMAT"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.unknown

touch $CLICKHOUSE_TEST_UNIQUE_NAME.xml
$CLICKHOUSE_LOCAL -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.x*')" 2>&1 | grep -c "XML file format doesn't support schema inference"
rm $CLICKHOUSE_TEST_UNIQUE_NAME.xml
