#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Force an error (code 395 = FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) to populate system.errors.
$CLICKHOUSE_CLIENT -q "SELECT throwIf(true, '04040_errors_table_symbols_lines')" 2>/dev/null

# `last_error_symbols` is resolved from the symbol table, which is present on all our builds (Linux
# and macOS), so the demangled Exception frames must be visible everywhere.
$CLICKHOUSE_CLIENT -q "
SELECT arrayExists(x -> x LIKE '%Exception%', last_error_symbols)
FROM system.errors WHERE code = 395 ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"

# `last_error_lines` additionally needs DWARF debug info: read from the binary on Linux, or from a
# co-located .dSYM bundle on macOS. The macOS CI runner ships without a .dSYM, so file:line is
# unavailable there - the same limitation as 02420_stracktrace_debug_symbols. Assert the file:line
# info where it is available, and assert its real absence on macOS, rather than skipping or faking.
if [ "$($CLICKHOUSE_CLIENT -q "SELECT value = 'Darwin' FROM system.build_options WHERE name = 'SYSTEM'")" = "1" ]; then
    $CLICKHOUSE_CLIENT -q "
    SELECT NOT arrayExists(x -> x LIKE '%:%:%', last_error_lines)
    FROM system.errors WHERE code = 395 ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"
else
    $CLICKHOUSE_CLIENT -q "
    SELECT arrayExists(x -> x LIKE '%:%:%', last_error_lines)
    FROM system.errors WHERE code = 395 ORDER BY last_error_time DESC LIMIT 1 FORMAT TSV"
fi
