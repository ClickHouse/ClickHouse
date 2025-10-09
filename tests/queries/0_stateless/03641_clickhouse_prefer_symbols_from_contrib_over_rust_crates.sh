#!/usr/bin/env bash
# Tags: long, no-msan

# FIXME: Note, this is not a queries test at all, move it somewhere else

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
SELECT replaceRegexpOne(unit_name, '.*(contrib)', '\1')
FROM file('$(which $CLICKHOUSE_BINARY)', DWARF)
WHERE name IN ('ZSTD_compressStream2', 'LZ4_compress_fast_extState') AND not(has(attr_name, 'declaration'))
ORDER BY ALL
SETTINGS max_threads=64
"
