#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This is parsed as text.
$CLICKHOUSE_LOCAL -q "select toString(424242424242424242424242424242424242424242424242424242::UInt256) as x format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet --structure='x UInt256' -q "select * from table"

# But this is parsed as binary because text length happens to be 32 bytes. Not ideal.
$CLICKHOUSE_LOCAL -q "select toString(42424242424242424242424242424242::UInt256) as x format Parquet" | $CLICKHOUSE_LOCAL --input-format=Parquet --structure='x UInt256' -q "select * from table"
