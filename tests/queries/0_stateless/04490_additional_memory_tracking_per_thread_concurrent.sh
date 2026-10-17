#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the deadlock observed when a pipeline lambda's throw
# happens before the query's thread group is attached: in that case the job
# was skipped and the pipeline consumer blocked forever on the empty output
# queue. The original trigger was `additional_memory_tracking_per_thread`
# reserving memory before the switch; the fix routes the throw through each
# pipeline lambda's own try/catch after `ThreadGroupSwitcher`, so the failure
# propagates as a normal query error regardless of where the limit is hit.
#
# Stateless tests run with the production default (4 MiB) for the setting.
# To make the failure path independent of that reservation we force the
# same exception with `max_untracked_memory = 0`: the very first in-query
# allocation is reported and exceeds `max_memory_usage = 1`. Each launched
# query must finish (with or without a memory-limit error) within `timeout`
# -- a hang would manifest as the timeout firing.

# `N` background clients race to exercise the throw-before-thread-group-attach
# path. Each client itself spawns `max_threads = 4` workers, so `N * 4 = 128`
# worker jobs is enough to surface the original deadlock reliably.
#
# `TIMEOUT` is set generously to absorb the per-process startup tax: under
# parallel CI stress (flaky-check re-runs the test alongside many others on
# the same host) it can take a long time for every client to even connect.
# A hang in the regressed code path would not finish at all -- finishing
# slowly under load is fine.
N=32
TIMEOUT=60

for _ in $(seq 1 $N); do
    timeout "$TIMEOUT" $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM numbers_mt(10000)
        SETTINGS max_threads = 4, max_memory_usage = 1, max_untracked_memory = 0
    " > /dev/null 2>&1 &
done

# Wait for every background client; record any that did not exit
# (i.e. the timeout fired -- which would indicate a hang).
# Capture the real exit status from `wait` (we need the child's actual code,
# not the result of a negated check). Redirect stderr to suppress bash
# job-control messages ("Killed", "Terminated") that would otherwise
# contaminate the test's stderr.
hung=0
for pid in $(jobs -p); do
    rc=0
    wait "$pid" 2>/dev/null || rc=$?
    # 124 = exit code from `timeout` when the timeout actually fires.
    # Any other non-zero is fine (likely 241 / MEMORY_LIMIT_EXCEEDED).
    if [ "$rc" -eq 124 ]; then
        hung=$((hung + 1))
    fi
done

echo "hung=$hung"

# After the storm, the server must still answer trivial queries.
$CLICKHOUSE_CLIENT --query "SELECT 'survived'"
