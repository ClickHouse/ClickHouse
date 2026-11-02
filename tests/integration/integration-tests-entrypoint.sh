#!/usr/bin/env bash

JEMALLOC_PROFILER=0
if [[ ! -v MALLOC_CONF ]]; then
    jemalloc_profiles=/tmp/jemalloc_profiles
    mkdir -p "$jemalloc_profiles"

    export MALLOC_CONF=prof_active:true,prof_prefix:$jemalloc_profiles/clickhouse.jemalloc
    JEMALLOC_PROFILER=1
fi

PID=0

function handle_term()
{
    echo "Sending TERM to $PID"
    ps aux
    kill -TERM "$PID"
}
trap handle_term TERM

echo "Running: $*"
"$@" &
PID=$!
# This will be interrupted by SIGTERM that is received by this script
wait $PID
server_exit_code=$?

# How long the server is given to finish on its own before its stacks are dumped.
GDB_ATTACH_DEADLINE=60

# `docker compose stop` signals every container of a cluster at once, so every node that outlives
# the deadline above reaches gdb at the same moment. gdb has to read the whole binary's debug info
# to walk the stacks - measured at 3.4 GiB of anonymous memory on an ASan build and 4.3 GiB on the
# coverage one - so a nine-node cluster asks for around 40 GiB, which is the entire budget of the
# cgroup that holds the nested containers (`CI_DIND_NESTED_BUDGET`, see
# ci/jobs/scripts/docker_in_docker.sh). The kernel then kills the gdb processes and the job reports
# `Container memory budget exceeded (/docker)` with no stacktrace to show for it, so attaching one
# at a time is not a restriction but the only way this dump produces anything at all. The lock sits
# on the repository mount because that is the only path every ClickHouse container of every pytest
# worker shares, which is what bounds the job as a whole rather than one cluster.
GDB_ATTACH_LOCK=/debug/ci/tmp/integration-tests-gdb-attach.lock

# The wait has to leave room inside `stop_grace_period` (10m, see helpers/cluster.py) for the
# deadline above and for gdb itself, or docker would kill the container mid-dump and the queue
# would produce nothing at all. A node that does not get its turn in time says so and is skipped:
# the first stacktraces are what a shutdown hang is diagnosed from, and the nodes behind them are
# waiting on the same thing.
GDB_ATTACH_LOCK_WAIT=240

function dump_stacktraces_on_shutdown()
{
    # GDB_ATTACH_DEADLINE sec should be enough to finish the server
    for _ in $(seq "$GDB_ATTACH_DEADLINE"); do
        if ! kill -0 "$PID" 2>/dev/null; then
            return
        fi
        sleep 1
    done

    if ! kill -0 "$PID" 2>/dev/null; then
        return
    fi

    mkdir -p "$(dirname "$GDB_ATTACH_LOCK")"
    (
        if ! flock -w "$GDB_ATTACH_LOCK_WAIT" 9; then
            echo "No gdb attach lock after ${GDB_ATTACH_LOCK_WAIT}s, skipping thread stacktraces"
            exit 0
        fi
        # Re-checked under the lock: a server that was merely slow rather than stuck exits while
        # its turn is waited for, and then there is nothing left to attach to.
        if ! kill -0 "$PID" 2>/dev/null; then
            echo "Server exited while waiting for the gdb attach lock"
            exit 0
        fi
        echo "Attaching gdb to obtain thread stacktraces"
        gdb -batch -ex 'thread apply all bt' -p "$PID" > /var/log/clickhouse-server/stdout.log
    ) 9>"$GDB_ATTACH_LOCK"
}
dump_stacktraces_on_shutdown &

while kill -0 "$PID" 2>/dev/null; do
    wait $PID
    server_exit_code=$?
done
echo "Server exited with $server_exit_code"

# Wait dump_stacktraces_on_shutdown
wait

if [[ $JEMALLOC_PROFILER -eq 1 ]]; then
    jemalloc_reports=/var/lib/clickhouse/jemalloc
    mkdir -p "$jemalloc_reports"

    echo "=== jemalloc reports:"
    ls -dlt "$jemalloc_profiles"/* | head

    bin="$(which clickhouse)"
    last_profile="$(ls -dt "$jemalloc_profiles"/* | head -1)"
    echo "Using $last_profile"

    if [[ -n $last_profile ]]; then
        jeprof "$bin" "$last_profile" --text > "$jemalloc_reports/jemalloc.txt"
        jeprof "$bin" "$last_profile" --collapsed | flamegraph.pl --color mem --width 2560 > "$jemalloc_reports/jemalloc.svg"
    fi
fi

chmod -R a+rX /var/log/clickhouse-server 2>/dev/null || true

# Preserve exit code of the server
exit $server_exit_code
