#!/bin/bash
# Handle a hung test process: capture all-thread stacktraces, then kill it.
#
# Usage: handle-hang.sh <pid> <artifact_dir> [--progress-file PATH]
#
# Auto-detects the versioned lldb binary (e.g. lldb-21).
# Auto-numbers the output file (stacktrace-NNN.txt) based on the highest
# iteration number found in the progress file and existing artifacts.
#
# Steps performed:
#   1. Verify process is alive
#   2. Find lldb
#   3. Compute next iteration number (from progress file + existing artifacts)
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
    echo "Usage: $0 <pid> <artifact_dir> [--progress-file PATH]" >&2
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

PID="$1"
ARTIFACT_DIR="$2"
shift 2

PROGRESS_FILE="_clean-tsan/progress.md"

while [ $# -gt 0 ]; do
    case "$1" in
        --progress-file)
            PROGRESS_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

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

# Compute next iteration number.
# Sources: progress.md ("## Iteration NNN") and existing artifact files.
max_iter=0

if [ -f "$PROGRESS_FILE" ]; then
    while IFS= read -r num; do
        num=$((10#$num))  # strip leading zeros
        if [ "$num" -gt "$max_iter" ]; then
            max_iter=$num
        fi
    done < <(grep -oP '## Iteration \K[0-9]+' "$PROGRESS_FILE" 2>/dev/null || true)
fi

# Also check existing artifact files in the target directory
if [ -d "$ARTIFACT_DIR" ]; then
    for f in "$ARTIFACT_DIR"/*.txt; do
        [ -e "$f" ] || continue
        num=$(echo "$f" | grep -oP '[0-9]{3}(?=\.txt$)' || true)
        if [ -n "$num" ]; then
            num=$((10#$num))
            if [ "$num" -gt "$max_iter" ]; then
                max_iter=$num
            fi
        fi
    done
fi

next_iter=$((max_iter + 1))
NNN=$(printf '%03d' "$next_iter")
echo "ITERATION=$NNN"

# Create artifact directory
mkdir -p "$ARTIFACT_DIR"

# Capture all-thread stacktraces
OUTFILE="$ARTIFACT_DIR/stacktrace-$NNN.txt"
sudo "$LLDB" -p "$PID" -o "bt all" -o "detach" -o "quit" > "$OUTFILE" 2>&1
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
