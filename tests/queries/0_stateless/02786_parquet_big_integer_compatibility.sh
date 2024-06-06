#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "select toString(424242424242424242424242424242424242424242424242424242::UInt256) as x format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet --structure='x UInt256' -q "select * from table"

