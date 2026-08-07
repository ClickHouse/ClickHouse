#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

printf '1\t' | $CLICKHOUSE_LOCAL --query="SELECT * FROM table" --structure='a String, b String'
printf '1\t' | $CLICKHOUSE_LOCAL --input_format_null_as_default 0 --query="SELECT * FROM table" --structure='a String, b String'
printf '1\t' | $CLICKHOUSE_LOCAL --input_format_null_as_default 1 --query="SELECT * FROM table" --structure='a String, b String'
printf '1\t' | $CLICKHOUSE_LOCAL --query="SELECT * FROM table" --structure='a String, b Nullable(String)'
printf '1\t' | $CLICKHOUSE_LOCAL --input_format_null_as_default 0 --query="SELECT * FROM table" --structure='a String, b Nullable(String)'
printf '1\t' | $CLICKHOUSE_LOCAL --input_format_null_as_default 1 --query="SELECT * FROM table" --structure='a Nullable(String), b Nullable(String)'
