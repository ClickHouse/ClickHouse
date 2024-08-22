#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists insert_select_progress_tcp;
    create table insert_select_progress_tcp(s UInt16) engine = MergeTree order by s;
"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CURDIR"/02458_insert_select_progress_tcp.python

$CLICKHOUSE_CLIENT -q "drop table insert_select_progress_tcp"
