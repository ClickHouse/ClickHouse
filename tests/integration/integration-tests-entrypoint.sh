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

echo "Runnig: $*"
"$@" &
PID=$!
# This will be interrupted by SIGTERM that is received by this script
wait $PID
server_exit_code=$?

function dump_stacktraces_on_shutdown()
{
    # 60 sec should be enough to finish the server
    for _ in {1..60}; do
        if ! kill -0 "$PID" 2>/dev/null; then
            return
        fi
        sleep 1
    done

    if kill -0 "$PID"; then
        echo "Attaching gdb to obtain thread stacktraces"
        gdb -batch -ex 'thread apply all bt' -p "$PID" > /var/log/clickhouse-server/stdout.log
    fi
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

# Preserve exit code of the server
exit $server_exit_code
