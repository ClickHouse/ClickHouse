#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# max_execution_time is a safety belt so a regression fails fast instead of hanging the suite. The
# assertion is always the error code or the row count, never the elapsed time.
BOUND="SETTINGS max_execution_time = 30"

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# Append an Avro block header (zigzag object count, zigzag byte count) plus the file's own sync
# marker, so the appended block is well-framed and only its declared count is wrong.
# The byte count defaults to zero, which is what an empty appended payload declares.
avro_append_block() {
    python3 -c "
import sys
src, dst, count, bytes_declared = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
def zigzag(n):
    n = ((n << 1) ^ (n >> 63)) if n < 0 else (n << 1)
    n &= (1 << 64) - 1
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            break
    return bytes(out)
data = open(src, 'rb').read()
open(dst, 'wb').write(data + zigzag(count) + zigzag(bytes_declared) + data[-16:])
" "$1" "$2" "$3" "${4-0}"
}

$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toString(number) AS s FROM numbers(5000)
    INTO OUTFILE '$DIR/ok.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'null'"
# An all-NULL column encodes to zero payload bytes, so this file legitimately declares 1000 rows in
# a few hundred bytes. It is the counter-example to bounding the declared count by the input size.
$CLICKHOUSE_LOCAL -q "
    SELECT NULL AS a FROM numbers(1000)
    INTO OUTFILE '$DIR/nullrows.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'null'"
# Same for a compressed file: 200000 rows of one repeated value compress to under a kilobyte.
$CLICKHOUSE_LOCAL -q "
    SELECT 1 AS a FROM numbers(200000)
    INTO OUTFILE '$DIR/deflate.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'deflate'"

# A negative declared object count. Never reaches zero by decrementing, so the count call used to
# spin here until the query deadline.
avro_append_block "$DIR/ok.avro" "$DIR/negative.avro" -5 0
# A negative declared byte count. Widens into an unbounded payload limit when cast unsigned.
avro_append_block "$DIR/ok.avro" "$DIR/negbytes.avro" 10 -1
# A positive declared count larger than the payload holds. The count must not be answered from the
# header alone, or every row reported past the payload is invented and returned as a success.
avro_append_block "$DIR/ok.avro" "$DIR/huge.avro" 1000000000 0

# Assert on the error class, not on one message: a corrupted header is rejected either by the Avro
# library at the block header or by the read path when the payload runs out.
echo '--- a corrupted block header is rejected instead of counted'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    for f in negative.avro negbytes.avro huge.avro; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count() FROM file('$DIR/$f', Avro)
            $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'AVRO_EXCEPTION'
    done
done

# A block whose declared count is corrupted is also a block that runs out of input, so the broad
# assertion above cannot tell the header check from payload exhaustion. Name the header diagnostic
# for both counts, which keeps these pinned to the header check itself.
echo '--- a negative declared count is rejected at the header, not at the payload'
for setting in 1 0; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/negative.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'object count in block header'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/negbytes.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'byte count in block header'
done

echo '--- valid input still counts every row, including rows that occupy no payload bytes'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    for f in ok.avro nullrows.avro deflate.avro; do
        $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/$f', Avro) $BOUND, optimize_count_from_files = $setting"
    done
done

echo '--- reading valid input is unchanged, at several block sizes'
for size in 1 13 65505; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(n, s))
        FROM file('$DIR/ok.avro', Avro)
        $BOUND, max_block_size = $size"
done

rm -rf "$DIR"
