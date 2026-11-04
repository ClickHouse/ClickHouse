#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The deadly signal handlers and the signal listener are installed before the global context
# exists, so the crash handler has to tolerate a global context that is not there yet.
#
# `--logger.log` is opened while the configuration is being processed, which is still before the
# global context is created, and opening a FIFO for writing blocks until a reader appears. That
# parks the process in exactly that window with the listener already running, so a signal
# delivered there runs the crash handler in the state under test. The signal also interrupts the
# blocked open, so the process reports a file error and exits on its own afterwards.
#
# `SIGTSTP` is the signal to use: the handler treats it as non-crashing and returns, so the manner
# of the exit is the oracle. A fault signal would end the process even when the handler works,
# leaving nothing to distinguish.
#
# Unique names keep this parallel-safe.
fifo="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05042_logger.fifo"
err="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05042_stderr.txt"
work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05042_work"
rm -rf "$fifo" "$err" "$work"
mkfifo "$fifo"
mkdir -p "$work"

cleanup() {
    [ -n "${local_pid:-}" ] && kill -9 "$local_pid" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$fifo" "$err" "$work"
}
trap cleanup EXIT

# Nothing reads the FIFO, so this parks in its open.
$CLICKHOUSE_LOCAL --path="$work" --logger.log="$fifo" --logger.level=trace \
    --query "SELECT 1" >/dev/null 2>"$err" &
local_pid=$!

# Both checks are mandatory. Signalling before the handlers are installed would use the default
# disposition and only stop the process, and signalling after the global context has been created
# would exercise the state that already worked - either way nothing under test runs and the test
# would pass without measuring anything.
#
# The process announces the log destination on stderr immediately before opening it, so that line
# means it has reached the open. `wait_for_partner` is the kernel state for a process blocked
# opening a FIFO, and confirms it is parked there rather than elsewhere in startup; it needs symbol
# names in /proc, so sleeping is the fallback when it stays unavailable while the line is already
# out. A signal that still lands outside the window is caught by the oracles below.
parked=0
announced=0
for i in {1..600}; do
    kill -0 "$local_pid" 2>/dev/null || break
    grep -q "Logging trace to $fifo" "$err" 2>/dev/null && announced=1
    if [ "$(cat "/proc/$local_pid/wchan" 2>/dev/null)" = "wait_for_partner" ]; then
        parked=1
        break
    fi
    [ "$announced" = 1 ] && [ "$i" -gt 20 ] && { parked=1; break; }
    sleep 0.1
done
[ "$parked" = 1 ] || { echo "failed to park the process before the global context is created"; exit 1; }

# The working directory is populated right after the global context is created, so an empty one
# confirms the process has not passed that point.
[ -z "$(ls -A "$work" 2>/dev/null)" ] || { echo "the global context already exists"; exit 1; }

kill -TSTP "$local_pid"

wait "$local_pid"
rc=$?
local_pid=""

# The crash handler prints its first line before it reaches the global context, so this says the
# handler ran rather than merely that the signal was delivered.
grep -q 'Received signal' "$err" \
    && echo "crash handler ran" \
    || echo "crash handler did not run"

# Reaching the absent global context ends the process: a sanitizer build reports the null
# dereference and aborts, and every other build reaches a mutex at a wild address and takes a
# second signal inside the handler, which also costs the ~300s the faulting thread spends waiting
# for a report that can no longer come.
#
# `clickhouse-local` exits with a ClickHouse error code that the shell truncates to its low byte,
# which can land in the same numeric range as a signal status, so name the signal statuses instead
# of testing a range.
case "$rc" in
    134) echo "died from signal 6" ;;
    139) echo "died from signal 11" ;;
    *) echo "no signal death" ;;
esac

# The report has to be absent rather than merely unseen, so check for it directly. It is what a
# sanitizer build prints, and it is the only build that can print it.
grep -q 'member call on null pointer' "$err" \
    && echo "null global context dereferenced" \
    || echo "no null dereference"
