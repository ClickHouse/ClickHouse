#!/usr/bin/env bash
# Tags: no-replicated-database
# - no-replicated-database: Progress events are not streamed by DDL workers

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
# For 1 second sleep and 1ms of interactive_delay we definitelly should have non zero progress packet.
# NOTE: interactive_delay=0 cannot be used since in this case CompletedPipelineExecutor will not call cancelled callback.
python3 "$CURDIR"/helpers/parse_query_progress_tcp.python "create or replace table create_or_replace_progress_tcp engine = MergeTree() order by () as select sleep(1) from numbers(2) settings max_block_size=1, interactive_delay=1000"
