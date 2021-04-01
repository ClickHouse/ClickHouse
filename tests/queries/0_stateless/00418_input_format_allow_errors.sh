#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS formats_test"
$CLICKHOUSE_CLIENT --query="CREATE TABLE formats_test (x UInt64, s String) ENGINE = Memory"

echo -ne '1\tHello\n \n3\tGoodbye\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO formats_test FORMAT TSV"

$CLICKHOUSE_CLIENT --query="SELECT * FROM formats_test ORDER BY x, s"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=2 --input_format_allow_errors_ratio=0.1 --query="INSERT INTO formats_test FORMAT TSV"

$CLICKHOUSE_CLIENT --query="SELECT * FROM formats_test ORDER BY x, s"

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.4 --query="INSERT INTO formats_test FORMAT TSV" 2> /dev/null; echo $?

echo -ne '1\tHello\n2\n3\tGoodbye\n\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=1 --input_format_allow_errors_ratio=0.6 --query="INSERT INTO formats_test FORMAT TSV"

echo -ne 'x=1\ts=TSKV\nx=minus2\ts=trash1\ns=trash2\tx=-3\ns=TSKV Ok\tx=4\ns=trash3\tx=-5\n' | $CLICKHOUSE_CLIENT --input_format_allow_errors_num=3 -q "INSERT INTO formats_test FORMAT TSKV"

$CLICKHOUSE_CLIENT --query="SELECT * FROM formats_test ORDER BY x, s"

$CLICKHOUSE_CLIENT --query="DROP TABLE formats_test"
