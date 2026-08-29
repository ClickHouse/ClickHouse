#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on Snappy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A hadoop-snappy block starts with the big-endian uncompressed length of the block, so a run of zero
# bytes is a run of blocks that decode to nothing. Reading them used to recurse once per block.
ZEROS="${CLICKHOUSE_TMP}/05049_zeros.snappy"
VALID="${CLICKHOUSE_TMP}/05049_valid.snappy"
MIXED="${CLICKHOUSE_TMP}/05049_mixed.snappy"

head -c 2000000 /dev/zero > "$ZEROS"

$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$ZEROS', 'TabSeparated', 'x UInt8')"

$CLICKHOUSE_LOCAL --query "SELECT number FROM numbers(3) INTO OUTFILE '$VALID' TRUNCATE COMPRESSION 'snappy' FORMAT TabSeparated"
cat "$ZEROS" "$VALID" "$ZEROS" > "$MIXED"

$CLICKHOUSE_LOCAL --query "SELECT * FROM file('$MIXED', 'TabSeparated', 'x UInt8')"

rm -f "$ZEROS" "$VALID" "$MIXED"
