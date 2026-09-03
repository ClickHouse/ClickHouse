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
        # This test must actually open remote connections: it checks that max_distributed_connections
        # lets the 20 shards be read in parallel (so it finishes in ~1s instead of ~20s) even with
        # max_threads=1. With prefer_localhost_replica=1 (the default), a shard whose address is local
        # is served by the local replica and executed in-process - no remote connection - which then
        # runs serially under max_threads=1 and defeats the test.
        # On the macOS test runner 127.0.0.2+ are aliased on lo0, so they are local addresses and the
        # shards would be collapsed to local execution. Force remote connections so the test exercises
        # what it is meant to (remote connections behave differently from local ones).
        --prefer_localhost_replica 0
        --query "SELECT sum(sleepEachRow(1)) FROM remote('127.{2..21}', system.one)"
        --format Null
    )
    # 10 less then 20 seconds (20 streams), but long enough to cover possible load peaks
    # "$@" left to pass manual options (like --experimental_use_processors 0) during manual testing

    timeout 10s ${CLICKHOUSE_CLIENT} "${opts[@]}" "$@" && break
    ((++i))
done
