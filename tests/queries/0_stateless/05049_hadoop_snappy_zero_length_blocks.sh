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

# 26.3 has no hadoop-snappy writer, so build the valid block by hand: the big-endian uncompressed
# length (6), the big-endian compressed length (8), then the raw snappy stream for "0\n1\n2\n":
# a varint uncompressed length (0x06) and one literal tag (0x14 = 5 << 2, i.e. six bytes follow).
printf '\x00\x00\x00\x06\x00\x00\x00\x08\x06\x14\x30\x0a\x31\x0a\x32\x0a' > "$VALID"
cat "$ZEROS" "$VALID" "$ZEROS" > "$MIXED"

$CLICKHOUSE_LOCAL --query "SELECT * FROM file('$MIXED', 'TabSeparated', 'x UInt8')"

rm -f "$ZEROS" "$VALID" "$MIXED"
