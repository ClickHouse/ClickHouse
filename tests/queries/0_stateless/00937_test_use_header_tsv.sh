#!/usr/bin/env bash
# shellcheck disable=SC2016

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.tsv"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test.tsv (d Date, u UInt8, str String) ENGINE = TinyLog"

INSERT_QUERY='$CLICKHOUSE_CLIENT --query="INSERT INTO test.tsv FORMAT TSVWithNames"'
USE_HEADER='--input_format_with_names_use_header=1'
SKIP_UNKNOWN='--input_format_skip_unknown_fields=1'

# Simple check for parsing
echo -ne 'd\tu\tstr\n2019-04-18\t42\tLine1\n2019-04-18\t42\tLine2' | eval "$INSERT_QUERY"
echo -ne 'd\tu\tstr\n2019-04-18\t42\tLine3\n2019-04-18\t42\tLine4' | eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'd\tu\tstr\n2019-04-18\t42\tLine5\n2019-04-18\t42\tLine6' | eval "$INSERT_QUERY" $USE_HEADER $SKIP_UNKNOWN

# Random order of fields
echo -ne 'u\td\tstr\n42\t2019-04-18\tLine7\n' | eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'u\tstr\td\n42\tLine8\t2019-04-18\n' | eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'str\tu\td\nLine9\t42\t2019-04-18\n' | eval "$INSERT_QUERY" $USE_HEADER

# Excessive fields
echo -ne 'd\tu\tstr\tmore\tunknown\tfields\n2019-04-18\t1\tLine10\t\t\t\n2019-04-18\t2\tLine11\t\t\t\n' \
| eval "$INSERT_QUERY" $USE_HEADER $SKIP_UNKNOWN
echo -ne 'd\tunknown\tstr\tmore\tu\tfields\n2019-04-18\tblahblah\tLine12\t\t1\t\n2019-04-18\t\tLine13\tblahblah\t2\t\n' \
| eval "$INSERT_QUERY" $USE_HEADER $SKIP_UNKNOWN

# Missing fields (defaults)
echo -ne 'd\tu\n2019-04-18\t1\n2019-04-18\t2\n'            | eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'str\tu\nLine16\t1\nLine17\t2\n'                  | eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'd\tstr\n2019-04-18\tLine18\n2019-04-18\tLine19\n'| eval "$INSERT_QUERY" $USE_HEADER
echo -ne 'unknown\n\n\n'                                   | eval "$INSERT_QUERY" $USE_HEADER $SKIP_UNKNOWN

$CLICKHOUSE_CLIENT --query="SELECT * FROM test.tsv"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.tsv"
