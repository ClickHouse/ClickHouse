#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME
TEMPLATE_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.template

echo "42 | 43
Error line" > $DATA_FILE
echo '${a:CSV} | ${b:CSV}' > $TEMPLATE_FILE

$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', Template, 'a UInt32, b UInt32') settings format_template_row='$TEMPLATE_FILE', input_format_allow_errors_num=1"

rm $DATA_FILE
rm $TEMPLATE_FILE

