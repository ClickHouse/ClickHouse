#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -n -e "name,value\nHello,123\nWorld,456" | ${CLICKHOUSE_LOCAL} --input-format CSVWithNames --input_format_csv_allow_variable_number_of_columns 1 --structure "name String, value UInt32, test String DEFAULT name || ', ' || value" --output-format TSV --copy
echo -n -e "name,value,test\nHello,123,x\nWorld,456" | ${CLICKHOUSE_LOCAL} --input-format CSVWithNames --input_format_csv_allow_variable_number_of_columns 1 --structure "name String, value UInt32, test String DEFAULT name || ', ' || value" --output-format TSV --copy
