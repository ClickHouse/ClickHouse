#!/bin/bash
# Run a ClickHouse unit test under TSan with hang detection.
#
# Usage: run-unittest.sh <gtest_filter> <log_file> [--repeat N]
#
# Launches the test binary in background, polls the log file for stalled
# output (hang detection), and reports the outcome via structured key=value
# lines on stdout.
#
# Output (always):
#   PID=<pid>
#
# Output on normal exit:
#   EXIT_CODE=<code>
#
# Output on hang:
#   HANG_DETECTED=1
#   HUNG_TEST=<last [ RUN ] line>
#   CPU=<percent>
#
# The process is left alive on hang so that the caller can attach lldb
# before killing it. The caller is responsible for killing the process.
#
# Exit code: always 0 (check output lines for outcome).

set -euo pipefail

usage() {
    echo "Usage: $0 <gtest_filter> <log_file> [--repeat N]" >&2
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

FILTER="$1"
LOG_FILE="$2"
shift 2

REPEAT=20
POLL_INTERVAL=10

while [ $# -gt 0 ]; do
    case "$1" in
        --repeat)
            REPEAT="$2"
            shift 2
            ;;
        --poll-interval)
            POLL_INTERVAL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

BINARY="./build_tsan/src/unit_tests_dbms"
if [ ! -x "$BINARY" ]; then
    echo "ERROR: Binary not found: $BINARY" >&2
    exit 1
fi

# Find llvm-symbolizer matching the compiler version used for the TSan build.
# Without this, TSan falls back to addr2line which is extremely slow.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SYMBOLIZER=$("$SCRIPT_DIR/detect-llvm-tool.sh" llvm-symbolizer 2>/dev/null || true)

TSAN_OPTS="halt_on_error=1 second_deadlock_stack=1 history_size=7"
if [ -n "$SYMBOLIZER" ]; then
    TSAN_OPTS="$TSAN_OPTS external_symbolizer_path=$SYMBOLIZER"
fi

# Launch test in background
TSAN_OPTIONS="$TSAN_OPTS" \
    "$BINARY" \
    --gtest_filter="$FILTER" \
    --gtest_repeat="$REPEAT" \
    --gtest_break_on_failure \
    > "$LOG_FILE" 2>&1 &
pid=$!
echo "PID=$pid"

# Monitor for hang: if the last line of the log file doesn't change
# between two consecutive checks, the test is hung.
prev_line=""
while kill -0 "$pid" 2>/dev/null; do
    sleep "$POLL_INTERVAL"
    curr_line=$(tail -1 "$LOG_FILE" 2>/dev/null || true)
    if [ "$curr_line" = "$prev_line" ] && [ -n "$curr_line" ]; then
        break  # Output stalled — test is hung
    fi
    prev_line="$curr_line"
done

if kill -0 "$pid" 2>/dev/null; then
    # Process still alive — hang detected.
    # Do NOT kill: caller needs to attach lldb first.
    cpu=$(ps -p "$pid" -o %cpu --no-headers 2>/dev/null | tr -d ' ')
    hung_test=$(grep '^\[ RUN' "$LOG_FILE" | tail -1 || true)
    echo "HANG_DETECTED=1"
    echo "HUNG_TEST=$hung_test"
    echo "CPU=$cpu"
else
    # Process exited normally — capture exit code before it's lost
    exit_code=0
    wait "$pid" 2>/dev/null || exit_code=$?
    echo "EXIT_CODE=$exit_code"
fi
