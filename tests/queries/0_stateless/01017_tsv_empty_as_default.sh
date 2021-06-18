#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS empty_as_default";
$CLICKHOUSE_CLIENT --query="CREATE TABLE empty_as_default (s String, n UInt64 DEFAULT 1, d Date DEFAULT '2019-06-19') ENGINE = Memory";

echo -ne 'abcd\t100\t2016-01-01
default\t\t
\t\t
default-eof\t\t' | $CLICKHOUSE_CLIENT --input_format_defaults_for_omitted_fields=1 --input_format_tsv_empty_as_default=1 --query="INSERT INTO empty_as_default FORMAT TSV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM empty_as_default ORDER BY s";
$CLICKHOUSE_CLIENT --query="DROP TABLE empty_as_default";
