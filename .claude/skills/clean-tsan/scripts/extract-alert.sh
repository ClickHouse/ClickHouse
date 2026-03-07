#!/bin/bash
# Extract the first TSan alert from a log file into a numbered artifact.
#
# Usage: extract-alert.sh <log_file> <artifact_dir> [--progress-file PATH]
#
# Finds the first WARNING/SUMMARY marker pair and extracts it to
# alert-NNN.txt. Auto-numbers based on progress file and existing artifacts.
#
# Output:
#   ALERT_COUNT=<total alerts in log>
#   ITERATION=<NNN>
#   ALERT_FILE=<full path to extracted alert>
#   ALERT_TYPE=<type from SUMMARY line, e.g. "data-race", "lock-order-inversion">
#
# Exit code: 0 on success, 1 if no alerts found.

set -euo pipefail

usage() {
    echo "Usage: $0 <log_file> <artifact_dir> [--progress-file PATH]" >&2
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

LOG_FILE="$1"
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

if [ ! -f "$LOG_FILE" ]; then
    echo "ERROR: Log file not found: $LOG_FILE" >&2
    exit 1
fi

# Count total alerts
alert_count=$(grep -c "SUMMARY: ThreadSanitizer:" "$LOG_FILE" 2>/dev/null || true)
echo "ALERT_COUNT=${alert_count:-0}"

if [ "${alert_count:-0}" -eq 0 ]; then
    echo "ERROR: No TSan alerts found in $LOG_FILE" >&2
    exit 1
fi

# Find line numbers of the first alert (WARNING ... SUMMARY pair)
markers=$(grep -n -E "WARNING: ThreadSanitizer:|SUMMARY: ThreadSanitizer:" "$LOG_FILE" | head -2)
start_line=$(echo "$markers" | head -1 | cut -d: -f1)
end_line=$(echo "$markers" | tail -1 | cut -d: -f1)

if [ -z "$start_line" ] || [ -z "$end_line" ]; then
    echo "ERROR: Could not find alert boundaries" >&2
    exit 1
fi

# Extract alert type from SUMMARY line
alert_type=$(sed -n "${end_line}p" "$LOG_FILE" | grep -oP 'SUMMARY: ThreadSanitizer: \K[^ ]+' || echo "unknown")
echo "ALERT_TYPE=$alert_type"

# Compute next iteration number
max_iter=0

if [ -f "$PROGRESS_FILE" ]; then
    while IFS= read -r num; do
        num=$((10#$num))
        if [ "$num" -gt "$max_iter" ]; then
            max_iter=$num
        fi
    done < <(grep -oP '## Iteration \K[0-9]+' "$PROGRESS_FILE" 2>/dev/null || true)
fi

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

# Create artifact directory and extract
mkdir -p "$ARTIFACT_DIR"
OUTFILE="$ARTIFACT_DIR/alert-$NNN.txt"
sed -n "${start_line},${end_line}p" "$LOG_FILE" > "$OUTFILE"
echo "ALERT_FILE=$OUTFILE"
