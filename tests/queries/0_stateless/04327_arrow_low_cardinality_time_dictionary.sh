#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: exporting LowCardinality(Time) to Arrow with the dictionary setting enabled must
# not throw and must round-trip.
#
# https://github.com/ClickHouse/ClickHouse/issues/104038
#
# Time maps to Arrow time32, but the Arrow dictionary writer cannot fill time values (its dispatcher
# has no time entry), and a time *dictionary* would in any case be unreadable by ClickHouse (it
# re-imports as LowCardinality(Time64), which DataTypeLowCardinality rejects). So a LowCardinality(Time)
# column is written as a plain time32 column instead of a dictionary, which round-trips. Only Time is
# covered because LowCardinality(Time64) cannot be constructed (Time64 is not allowed inside
# LowCardinality). The value is a plain time of day, so the output is timezone-independent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.arrow

echo "=== LowCardinality(Time) -> Arrow (low_cardinality_as_dictionary=1) -> read back ==="
$CLICKHOUSE_LOCAL \
    --allow_suspicious_low_cardinality_types 1 \
    --output_format_arrow_low_cardinality_as_dictionary 1 \
    -q "SELECT CAST(if(number % 2, '01:00:00'::Time, '02:00:00'::Time) AS LowCardinality(Time)) AS t
        FROM numbers(4)
        INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Arrow"
$CLICKHOUSE_LOCAL -q "SELECT t, toTypeName(t) FROM file('$DATA_FILE', 'Arrow') ORDER BY t"

rm -f "$DATA_FILE"
