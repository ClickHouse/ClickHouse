#!/usr/bin/env bash
# Regression test for a lost update in `LocalConnection`: the `Progress` packet used to be built with
# `fetchAndResetPiecewiseAtomically` (which already zeroes every counter atomically) and then followed by a
# redundant `reset`. The pipeline keeps incrementing the same counters from its own threads while the main thread
# sits between those two statements, so whatever landed in that window was thrown away and the query
# under-reported `rows_read` in the `statistics` of the JSON output.
#
# The window is a handful of atomic operations wide, so a single ordinary query almost never hits it.
# `interactive_delay` controls how often the progress packet is assembled and `max_block_size` how large a single
# increment is, so the two together turn a once-in-a-thousand-runs flake into a reliable failure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for _ in {1..5}
do
    ${CLICKHOUSE_LOCAL} --interactive_delay 1000 --max_block_size 1000 \
        --query "SELECT count() FROM numbers_mt(20000000) FORMAT JSON" \
        | jq -c '.statistics.rows_read'
done
