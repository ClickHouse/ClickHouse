#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `system.errors` keeps one shared row per error code across the whole server, and code 395
# (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO, the default for `throwIf`) is thrown by many other stateless
# tests that run concurrently, so reading that row back would be racy and could validate a stack
# trace this test doesn't own. Use a dedicated custom error code instead so the row is ours alone:
# `system.errors` (unlike `system.error_log`) skips any code without a registered name, and a code
# above `ErrorCodes::END` collapses onto one shared overflow slot, so it has to be a real, registered
# error code that no other code path throws, not merely one that is rare (an obscure-but-live code,
# such as `AMBIGUOUS_HANDLER`, can still collide with another parallel stateless test that hits it).
custom_error_code=775 # STATELESS_WORKER_DISCOVERY_ERROR: registered, but no code path throws it

# Force an error to populate system.errors.
$CLICKHOUSE_CLIENT -q "SELECT throwIf(true, '04040_errors_table_symbols_lines', toInt16($custom_error_code)) SETTINGS allow_custom_error_code_in_throwif = 1" 2>/dev/null

# `last_error_symbols` is resolved from the symbol table, which is present on all our builds (Linux
# and macOS), so the demangled Exception frames must be visible everywhere.
$CLICKHOUSE_CLIENT -q "
SELECT arrayExists(x -> x LIKE '%Exception%', last_error_symbols)
FROM system.errors WHERE code = $custom_error_code ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"

# `last_error_lines` additionally needs DWARF debug info: read from the binary on Linux, or from a
# co-located .dSYM bundle on macOS. Probe the actual availability of line info instead of assuming
# it from the OS: if the server's own printed stack trace contains 'file:line:column' frames, then
# debug info is usable - the text stack trace and `last_error_lines` resolve source locations
# through the same DWARF machinery. Then assert that `last_error_lines` is populated exactly when
# line info is available, rather than skipping or faking the result.
if $CLICKHOUSE_CLIENT --stacktrace -q "SELECT throwIf(true, '04040_probe', toInt16($custom_error_code)) SETTINGS allow_custom_error_code_in_throwif = 1" 2>&1 | grep -q -P ':[0-9]+:[0-9]+: '; then
    $CLICKHOUSE_CLIENT -q "
    SELECT arrayExists(x -> x LIKE '%:%:%', last_error_lines)
    FROM system.errors WHERE code = $custom_error_code ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"
else
    $CLICKHOUSE_CLIENT -q "
    SELECT NOT arrayExists(x -> x LIKE '%:%:%', last_error_lines)
    FROM system.errors WHERE code = $custom_error_code ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"
fi

# A set built from a subquery must be ready when the condition is applied to the table.
query_id_in="04040_in_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$query_id_in" -q "
SELECT count() FROM system.errors
WHERE code IN (SELECT toInt32($custom_error_code)) AND NOT remote AND arrayExists(x -> x LIKE '%Exception%', last_error_symbols)
SETTINGS system_events_show_zero_values = 1"

# The condition sees only the rows the table shows: the hidden remote row of this code has value 0.
$CLICKHOUSE_CLIENT -q "
SELECT count() FROM system.errors
WHERE code = $custom_error_code AND intDiv(value, value) = 1 AND arrayExists(x -> x LIKE '%Exception%', last_error_symbols)
SETTINGS system_events_show_zero_values = 0"

# Rows the condition rejects do not leave the table, so they are not symbolized. Zero rows are shown, so every
# registered code is a row the condition has to reject. The `OR` reaches the table only if every column in it is
# filterable, and only its `name` branch matches. Only `last_error_lines` is read: the `IN` probe reads `last_error_symbols`.
query_id_filtered="04040_filtered_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query_id "$query_id_filtered" -q "
SELECT last_error_lines FROM system.errors
WHERE NOT remote AND (name = 'STATELESS_WORKER_DISCOVERY_ERROR' OR code = -1 OR value > 1000000000000 OR last_error_time > now() + INTERVAL 1 DAY
    OR last_error_message = '04040' OR last_error_format_string = '04040' OR query_id = '04040')
SETTINGS system_events_show_zero_values = 1 FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND query_id = '$query_id_filtered'"
# One row of the table plus the one row the set's own subquery reads.
$CLICKHOUSE_CLIENT -q "
SELECT read_rows <= 2 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND query_id = '$query_id_in'"
