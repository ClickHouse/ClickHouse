#!/bin/bash
# Extract the first TSan alert from a log file into a numbered artifact.
#
# Usage: extract-alert.sh <log_file>
#
# Finds the first WARNING/SUMMARY marker pair and extracts it to
# _clean-tsan/NNN/alert.txt. Auto-numbers based on existing iteration
# subdirectories in _clean-tsan/.
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
    echo "Usage: $0 <log_file>" >&2
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

LOG_FILE="$1"

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

# Compute next iteration number from existing subdirectories (max + 1)
max_iter=$(find _clean-tsan -maxdepth 1 -type d -regex '.*/[0-9]+' -printf '%f\n' 2>/dev/null | sort -n | tail -1)
next_iter=$(( ${max_iter:-0} + 1 ))
NNN=$(printf '%03d' "$next_iter")
echo "ITERATION=$NNN"

# Create iteration directory and extract
mkdir -p "_clean-tsan/$NNN"
OUTFILE="_clean-tsan/$NNN/alert.txt"
sed -n "${start_line},${end_line}p" "$LOG_FILE" > "$OUTFILE"
echo "ALERT_FILE=$OUTFILE"
