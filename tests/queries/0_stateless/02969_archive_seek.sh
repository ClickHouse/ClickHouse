#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select sum(cityHash64(*)) from file('$CURDIR/data_parquet/02969.zip :: u.parquet') settings max_threads=4, max_read_buffer_size=1000"