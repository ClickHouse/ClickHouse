#!/bin/bash
# Handle a hung test process: capture all-thread stacktraces, then kill it.
#
# Usage: handle-hang.sh <pid>
#
# Auto-detects the versioned lldb binary (e.g. lldb-21).
# Auto-numbers the iteration directory (_clean-tsan/NNN/) based on
# existing subdirectories.
#
# Steps performed:
#   1. Verify process is alive
#   2. Find lldb
#   3. Compute next iteration number
#   4. Capture all-thread stacktraces via lldb (bt all)
#   5. Kill the process
#
# Output:
#   LLDB=<path to lldb binary>
#   ITERATION=<NNN>
#   STACKTRACE_FILE=<full path to stacktrace file>
#   KILLED=1 (or KILLED=0 if the process could not be killed)
#
# Requires sudo for ptrace attach.
# Exit code: 0 on success, 1 on error.

set -euo pipefail

usage() {
    echo "Usage: $0 <pid>" >&2
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

PID="$1"

# Verify process is alive
if ! kill -0 "$PID" 2>/dev/null; then
    echo "ERROR: Process $PID is not running" >&2
    exit 1
fi

# Find lldb matching the compiler version used for the TSan build.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LLDB=$("$SCRIPT_DIR/detect-llvm-tool.sh" lldb 2>/dev/null || true)
if [ -z "$LLDB" ]; then
    echo "ERROR: No lldb found matching the compiler in build_tsan/CMakeCache.txt" >&2
    exit 1
fi
echo "LLDB=$LLDB"

# Compute next iteration number from existing subdirectories (max + 1)
max_iter=$(find _clean-tsan -maxdepth 1 -type d -regex '.*/[0-9]+' -printf '%f\n' 2>/dev/null | sort -n | tail -1)
next_iter=$(( ${max_iter:-0} + 1 ))
NNN=$(printf '%03d' "$next_iter")
echo "ITERATION=$NNN"

# Create iteration directory
mkdir -p "_clean-tsan/$NNN"

# Capture all-thread stacktraces (don't let lldb failure prevent killing the process)
OUTFILE="_clean-tsan/$NNN/stacktrace.txt"
sudo "$LLDB" -p "$PID" -o "bt all" -o "detach" -o "quit" > "$OUTFILE" 2>&1 || true
echo "STACKTRACE_FILE=$OUTFILE"

# Kill the hung process and wait for it to exit.
# First try SIGTERM, then escalate to SIGKILL for deadlocked processes.
kill "$PID" 2>/dev/null || true
for _ in $(seq 1 50); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if kill -0 "$PID" 2>/dev/null; then
    # SIGTERM didn't work (likely deadlocked) — escalate to SIGKILL
    kill -9 "$PID" 2>/dev/null || true
    for _ in $(seq 1 30); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.1
    done
fi

if ! kill -0 "$PID" 2>/dev/null; then
    echo "KILLED=1"
else
    echo "KILLED=0"
    echo "WARNING: Process $PID could not be killed" >&2
fi
