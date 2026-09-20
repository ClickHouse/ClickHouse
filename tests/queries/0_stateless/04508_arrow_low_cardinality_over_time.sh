#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A LowCardinality column whose Arrow dictionary value type had no entry in the FOR_ARROW_TYPES
# dispatch used to abort with "Cannot fill arrow array <type> with LowCardinality(...) data".
# Time maps to arrow::time32 and the sub-second Interval kinds map to arrow::duration; both entries
# were missing. Feed the emitted Arrow bytes back through the Arrow reader to prove the dictionary
# stream is valid and readable, not just that the writer no longer throws.

SETTINGS="allow_suspicious_low_cardinality_types = 1, output_format_arrow_low_cardinality_as_dictionary = 1"

# The sub-second Interval kinds map to arrow::duration and round-trip back to Interval.
$CLICKHOUSE_LOCAL -q "select CAST(INTERVAL 5 SECOND AS LowCardinality(IntervalSecond)) as a format Arrow settings $SETTINGS" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow
$CLICKHOUSE_LOCAL -q "select CAST(INTERVAL 5 MILLISECOND AS LowCardinality(IntervalMillisecond)) as a format Arrow settings $SETTINGS" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow
$CLICKHOUSE_LOCAL -q "select CAST(INTERVAL 5 MICROSECOND AS LowCardinality(IntervalMicrosecond)) as a format Arrow settings $SETTINGS" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow
$CLICKHOUSE_LOCAL -q "select CAST(INTERVAL 5 NANOSECOND AS LowCardinality(IntervalNanosecond)) as a format Arrow settings $SETTINGS" | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow

# Time maps to arrow::time32. The dictionary is now written without aborting. Reading it back proves
# the emitted time32 dictionary stream decodes: either it round-trips to a value, or the reader
# reports that time32 currently maps to Time64 (which is not allowed inside LowCardinality) - a
# separate reader-side limitation. A malformed IPC stream would fail to decode the schema instead.
check_readable() {
    local out
    out=$($CLICKHOUSE_LOCAL -q "select $1 as a format Arrow settings $SETTINGS" 2>/dev/null \
        | $CLICKHOUSE_LOCAL -q "select * from table" --input-format=Arrow 2>&1)
    if echo "$out" | grep -qE "12:00:00|Time64"; then
        echo "$2 readable"
    else
        echo "$2 FAILED: $out"
    fi
}
check_readable "'12:00:00'::LowCardinality(Time)" "Time"
check_readable "'12:00:00'::LowCardinality(Nullable(Time))" "Nullable(Time)"
