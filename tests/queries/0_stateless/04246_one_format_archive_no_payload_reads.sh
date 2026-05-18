#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

archive_path="$CUR_DIR/data_parquet/02969.zip"

$CLICKHOUSE_LOCAL -q "select 'single', _file, count() from file('$archive_path :: u.parquet', 'One') group by _file"
$CLICKHOUSE_LOCAL -q "select 'glob', _file, count() from file('$archive_path :: *.parquet', 'One') group by _file order by _file"
$CLICKHOUSE_LOCAL -q "select 'missing', count() from file('$archive_path :: missing.parquet', 'One')"
