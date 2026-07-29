#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 128.1.2.0/24 is non-routable, so every connect blocks for the full connect timeout (see also
# 01361_fover_remote_num_tries). 40 addresses x 3 tries x 2 s is 240 s of dialing, against a 3 s
# max_execution_time: without a cancellation checkpoint in the connect-retry loop the query grinds
# through all of it and reports ALL_CONNECTION_TRIES_FAILED instead of the timeout.
START=$SECONDS
ERROR=$($CLICKHOUSE_CLIENT --connections_with_failover_max_tries 3 \
                           --connect_timeout_with_failover_ms 2000 \
                           --max_execution_time 3 \
                           --query "SELECT count() FROM remote('128.1.2.{1..40}', system.one) FORMAT Null" 2>&1)
ELAPSED=$((SECONDS - START))

# A bound, not a value: CI runners are heavily loaded. 60 s is far below the 240 s of dialing.
[ "$ELAPSED" -lt 60 ] && echo "stopped early" || echo "still dialing after ${ELAPSED}s"

# The reported code must be the cancellation, not the exhausted-retries outcome. TIMEOUT_EXCEEDED
# is a plain Exception, so it is not swallowed by the caller's `catch (const NetException &)`
# next-shard retry in getStructureOfRemoteTable.
echo "$ERROR" | grep -qF 'TIMEOUT_EXCEEDED' && echo "reported as timeout" || echo "wrong error: $ERROR"
