#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on Snappy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A hadoop-snappy block starts with the big-endian uncompressed length of the block, so a run of zero
# bytes is a run of blocks that decode to nothing. Reading them used to recurse once per block.
#
# The recursion is the only difference the fix makes: the reader returned the same bytes either way,
# so the crash is the signal. It reproduces wherever the self-call in `ReadBuffer::next` survives -
# the debug and sanitizer builds, which is where the crash was reported and which is what the bugfix
# validation runs against - while an optimizing build turns that self-call into a jump and survives.
# The assertions below additionally pin the skipping itself: no rows from a stream of empty blocks,
# and the rows around them preserved.
ZEROS="${CLICKHOUSE_TMP}/05049_zeros.snappy"
VALID="${CLICKHOUSE_TMP}/05049_valid.snappy"
MIXED="${CLICKHOUSE_TMP}/05049_mixed.snappy"

head -c 2000000 /dev/zero > "$ZEROS"

$CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$ZEROS', 'TabSeparated', 'x UInt8')"

$CLICKHOUSE_LOCAL --query "SELECT number FROM numbers(3) INTO OUTFILE '$VALID' TRUNCATE COMPRESSION 'snappy' FORMAT TabSeparated"
cat "$ZEROS" "$VALID" "$ZEROS" > "$MIXED"

$CLICKHOUSE_LOCAL --query "SELECT * FROM file('$MIXED', 'TabSeparated', 'x UInt8')"

rm -f "$ZEROS" "$VALID" "$MIXED"
