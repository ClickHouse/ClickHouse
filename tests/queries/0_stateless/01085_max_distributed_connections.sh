#!/usr/bin/env bash
# Tags: distributed, no-random-settings, no-msan
# no-msan: under MSan instrumentation the per-shard `only_analyze` Planner pass
# in `SelectStreamFactory::createForShardWithAnalyzer` costs ~140 ms × 20 shards
# = ~3 s, which combined with the staggered remote sub-query execution pushes
# the wall time over the 10 s `timeout` budget below.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

i=0 retries=300
# Sometimes ten seconds are not enough due to system overload.
# But if it can run in less than ten seconds at least sometimes - it is enough for the test.
while [[ $i -lt $retries ]]; do
    opts=(
        --max_distributed_connections 20
        --max_threads 1
        --query "SELECT sum(sleepEachRow(1)) FROM remote('127.{2..21}', system.one)"
        --format Null
    )
    # 10 less then 20 seconds (20 streams), but long enough to cover possible load peaks
    # "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing

    timeout 10s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@" && break
    ((++i))
done
