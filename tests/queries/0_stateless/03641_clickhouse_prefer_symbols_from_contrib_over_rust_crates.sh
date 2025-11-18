#!/usr/bin/env bash
# Tags: long, no-msan

# FIXME: Note, this is not a queries test at all, move it somewhere else

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check for DISABLE_ALL_DEBUG_SYMBOLS
if [ $($CLICKHOUSE_LOCAL -q "SELECT value LIKE '%-g0%' FROM system.build_options WHERE name = 'CXX_FLAGS'") -eq 1 ]; then
    echo "@@SKIP@@: No debug symbols"
    exit 0
fi
# MacOS does not have file:line (backtrace_symbols() does not provide this info)
if [ $($CLICKHOUSE_LOCAL -q "SELECT value = 'Darwin' FROM system.build_options WHERE name = 'SYSTEM'") -eq 1 ]; then
    echo "@@SKIP@@: MacOS does not have file:line in stacktraces"
    exit 0
fi

$CLICKHOUSE_LOCAL -q "
SELECT replaceRegexpOne(unit_name, '.*(contrib)', '\1')
FROM file('$(which $CLICKHOUSE_BINARY)', DWARF)
WHERE name IN ('ZSTD_compressStream2', 'LZ4_compress_fast_extState') AND not(has(attr_name, 'declaration'))
ORDER BY ALL
SETTINGS max_threads=64
"
