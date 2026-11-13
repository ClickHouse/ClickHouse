#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists tab;"
# Local disk: a part write through a write-through filesystem cache can take tens of seconds on slow lanes.
$CLICKHOUSE_CLIENT -q "create table tab(x LowCardinality(String)) engine = MergeTree order by tuple() settings storage_policy = 'default';"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CURDIR"/02010_lc_native.python

$CLICKHOUSE_CLIENT -q "drop table if exists tab;"
