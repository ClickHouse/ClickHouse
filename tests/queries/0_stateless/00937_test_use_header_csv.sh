#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.csv"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.csv (d Date, u UInt8, str String) ENGINE = TinyLog"

INSERT_QUERY='$CLICKHOUSE_CLIENT --query="INSERT INTO test.csv FORMAT CSVWithNames"'
USE_HEADER='--input_format_with_names_use_header=1'
SKIP_UNKNOWN='--input_format_skip_unknown_fields=1'

# Simple check for parsing
echo -ne 'd,u,str\n2019-04-18,42,Line1\n2019-04-18,42,Line2' | eval $INSERT_QUERY
echo -ne 'd,u,str\n2019-04-18,42,Line3\n2019-04-18,42,Line4' | eval $INSERT_QUERY $USE_HEADER
echo -ne 'd,u,str\n2019-04-18,42,Line5\n2019-04-18,42,Line6' | eval $INSERT_QUERY $USE_HEADER $SKIP_UNKNOWN

# Random order of fields
echo -ne 'u,d,str\n42,2019-04-18,Line7\n' | eval $INSERT_QUERY $USE_HEADER
echo -ne 'u,str,d\n42,Line8,2019-04-18\n' | eval $INSERT_QUERY $USE_HEADER
echo -ne 'str,u,d\nLine9,42,2019-04-18\n' | eval $INSERT_QUERY $USE_HEADER

# Excessive fields
echo -ne 'd,u,str,more,unknown,fields\n2019-04-18,1,Line10,,,\n2019-04-18,2,Line11,,,\n' \
| eval $INSERT_QUERY $USE_HEADER $SKIP_UNKNOWN
echo -ne 'd,unknown,str,more,u,fields\n2019-04-18,blahblah,Line12,,1,\n2019-04-18,,Line13,blahblah,2,\n' \
| eval $INSERT_QUERY $USE_HEADER $SKIP_UNKNOWN

# Missing fields (defaults)
echo -ne 'd,u\n2019-04-18,1\n2019-04-18,2\n'            | eval $INSERT_QUERY $USE_HEADER
echo -ne 'str,u\nLine16,1\nLine17,2\n'                  | eval $INSERT_QUERY $USE_HEADER
echo -ne 'd,str\n2019-04-18,Line18\n2019-04-18,Line19\n'| eval $INSERT_QUERY $USE_HEADER
echo -ne 'unknown\n\n\n'                                | eval $INSERT_QUERY $USE_HEADER $SKIP_UNKNOWN

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.csv"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.csv"
