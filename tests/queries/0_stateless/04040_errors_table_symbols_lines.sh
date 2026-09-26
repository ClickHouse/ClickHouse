#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `system.errors` keeps one shared row per error code across the whole server, and code 395
# (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO, the default for `throwIf`) is thrown by many other stateless
# tests that run concurrently, so reading that row back would be racy and could validate a stack
# trace this test doesn't own. Use a dedicated custom error code instead so the row is ours alone:
# `system.errors` (unlike `system.error_log`) skips any code without a registered name, and a code
# above `ErrorCodes::END` collapses onto one shared overflow slot, so it has to be a real, but
# obscure and unlikely-to-fire-concurrently, registered error code.
custom_error_code=1013 # AMBIGUOUS_HANDLER

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
