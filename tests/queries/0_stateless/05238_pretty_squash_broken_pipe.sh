#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pretty formats squash consecutive blocks and write them from a background thread. When that write
# fails (here: the reader of the pipe has exited), the error has to reach the query. Otherwise the
# query never learns that nobody reads its output and keeps running - this one would run forever.

# `output_format_pretty_max_rows` is raised so that every block is written, and the query keeps
# writing into the broken pipe. `output_format_pretty_squash_consecutive_ms` is pinned because the
# background writer is only used when it is non-zero. `timeout` exits with 124 if the query hangs.
timeout 60 $CLICKHOUSE_LOCAL --max_threads=1 --output_format_pretty_squash_consecutive_ms=50 --output_format_pretty_max_rows=1000000000000 \
    --query "SELECT number FROM numbers(1e18) FORMAT PrettyCompact" 2>/dev/null | head -n 1 > /dev/null

code=${PIPESTATUS[0]}
if [ "$code" -eq 124 ]
then
    echo "The query did not stop after its output pipe was broken"
else
    echo "OK"
fi
