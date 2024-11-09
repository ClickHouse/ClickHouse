#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# default output_format_pretty_max_rows is 10K
$CLICKHOUSE_LOCAL -q "select * from numbers(100e3) format PrettySpace settings max_threads=1, output_format_pretty_display_footer_column_names=0" | wc -l
