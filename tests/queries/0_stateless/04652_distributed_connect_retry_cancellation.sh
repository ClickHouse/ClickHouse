#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 198.51.100.0/24 is RFC 5737 TEST-NET-2: reserved for documentation and guaranteed never to be
# routed, so every connect blocks for the full connect timeout. 40 addresses x 3 tries x 2 s is
# 240 s of dialing, against a 3 s max_execution_time: without a cancellation checkpoint in the
# connect-retry loop the query grinds through all of it and reports ALL_CONNECTION_TRIES_FAILED
# instead of the timeout.
#
# use_hedged_requests selects a different IConnections implementation and is randomized by the test
# runner, so both values are pinned and run as separate cases. Settings go on the client command
# line, which survives a randomized `compatibility` setting.
run_case()
{
    local label=$1
    local hedged=$2
    local start=$SECONDS
    local error
    error=$($CLICKHOUSE_CLIENT --use_hedged_requests "$hedged" \
                               --connections_with_failover_max_tries 3 \
                               --connect_timeout_with_failover_ms 2000 \
                               --max_execution_time 3 \
                               --query "SELECT count() FROM remote('198.51.100.{1..40}', system.one) FORMAT Null" 2>&1)
    local elapsed=$((SECONDS - start))

    # A bound, not a value: the post-fix window is the 3 s deadline plus at most one 2 s connect
    # already in flight, and every extra dial adds another 2 s. 15 s therefore tolerates at most
    # (15-3)/2 = 6 extra dials while keeping ~3x margin over the observed elapsed, against 240 s of
    # pre-fix dialing.
    [ "$elapsed" -lt 15 ] && echo "$label stopped early" || echo "$label still dialing after ${elapsed}s"

    # The reported code must be the cancellation, not the exhausted-retries outcome. TIMEOUT_EXCEEDED
    # is a plain Exception, so it is not swallowed by the caller's `catch (const NetException &)`
    # next-shard retry in getStructureOfRemoteTable.
    echo "$error" | grep -qF 'TIMEOUT_EXCEEDED' && echo "$label reported as timeout" || echo "$label wrong error: $error"
}

run_case sync 0
run_case hedged 1
