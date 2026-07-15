#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

start_time=$($CLICKHOUSE_CLIENT -q "SELECT now()")

# system.error_log is created lazily, flush logs query makes it sure that the table is created.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS error_log;"

# Get the previous number of errors for 111, 222 and 333
errors_111=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 111")
errors_222=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 222")
errors_333=$($CLICKHOUSE_CLIENT -q "SELECT sum(value) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 333")

# Throw three random errors: 111, 222 and 333
$CLICKHOUSE_CLIENT -m -q "
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }
SYSTEM FLUSH LOGS error_log;
"

# Check that the three random errors are propagated
$CLICKHOUSE_CLIENT -m -q "
SELECT sum(value) > $errors_111 FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 111;
SELECT sum(value) > $errors_222 FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 222;
SELECT sum(value) > $errors_333 FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 333;
"

# Ensure that if we throw them again, they're still propagated
$CLICKHOUSE_CLIENT -m -q "
SELECT throwIf(true, 'error_log', toInt16(111)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 111 }
SELECT throwIf(true, 'error_log', toInt16(222)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 222 }
SELECT throwIf(true, 'error_log', toInt16(333)) SETTINGS allow_custom_error_code_in_throwif=1; -- { serverError 333 }
SYSTEM FLUSH LOGS error_log;
"

$CLICKHOUSE_CLIENT -m -q "
SELECT sum(value) > $(($errors_111+1)) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 111;
SELECT sum(value) > $(($errors_222+1)) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 222;
SELECT sum(value) > $(($errors_333+1)) FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 333;
"

# Test to check if columns with prefix last_error are populated
$CLICKHOUSE_CLIENT -m -q "
SELECT not isNull(last_error_time), last_error_query_id != '', last_error_message != ''
FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 333 ORDER BY last_error_time DESC LIMIT 1
FORMAT CSV;
"

$CLICKHOUSE_CLIENT -m -q "
SELECT arrayCount(x -> x != '', arrayMap(x -> demangle(addressToSymbol(x)), last_error_trace)) > 10
FROM system.error_log WHERE event_date >= yesterday() AND event_time >= '$start_time' AND code = 333 ORDER BY last_error_time DESC LIMIT 1
FORMAT CSV
SETTINGS allow_introspection_functions=1;
"

# Check that symbols and lines are populated.
# `last_error_symbols` is resolved from the symbol table, present on all our builds. `last_error_lines`
# additionally needs DWARF debug info - read from the binary on Linux, or from a co-located .dSYM
# bundle on macOS. The macOS CI runner ships without a .dSYM, so file:line is unavailable there (the
# same limitation as 02420_stracktrace_debug_symbols); assert its real absence on macOS rather than
# skipping or faking the result.
if [ "$($CLICKHOUSE_CLIENT -q "SELECT value = 'Darwin' FROM system.build_options WHERE name = 'SYSTEM'")" = "1" ]; then
    $CLICKHOUSE_CLIENT -m -q "
    SELECT
        arrayExists(x -> (x LIKE '%Exception%'), last_error_symbols),
        NOT arrayExists(x -> (x LIKE '%:%:%'), last_error_lines)
    FROM system.error_log
    WHERE code = 333 AND event_date >= yesterday() AND event_time >= (now() - 600)
    ORDER BY last_error_time DESC
    LIMIT 1
    FORMAT CSV
    "
else
    $CLICKHOUSE_CLIENT -m -q "
    SELECT
        arrayExists(x -> (x LIKE '%Exception%'), last_error_symbols),
        arrayExists(x -> (x LIKE '%:%:%'), last_error_lines)
    FROM system.error_log
    WHERE code = 333 AND event_date >= yesterday() AND event_time >= (now() - 600)
    ORDER BY last_error_time DESC
    LIMIT 1
    FORMAT CSV
    "
fi
