#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists insert_select_progress_tcp;
    create table insert_select_progress_tcp(s UInt16) engine = MergeTree order by s;
"

# We should have correct env vars from shell_config.sh to run this test
# For 1 second sleep and 1ms of interactive_delay we definitelly should have non zero progress packet.
# NOTE: interactive_delay=0 cannot be used since in this case CompletedPipelineExecutor will not call cancelled callback.
python3 "$CURDIR"/helpers/parse_query_progress_tcp.python "insert into function null('_ Int') select sleep(1) from numbers(2) settings max_block_size=1, interactive_delay=1000"

$CLICKHOUSE_CLIENT -q "drop table insert_select_progress_tcp"
