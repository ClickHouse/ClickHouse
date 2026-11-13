#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists tab;"
$CLICKHOUSE_CLIENT -q "create table tab(x LowCardinality(String)) engine = MergeTree order by tuple();"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CURDIR"/02010_lc_native.python

$CLICKHOUSE_CLIENT -q "drop table if exists tab;"
