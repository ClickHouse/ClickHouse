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
avro_append_block() {
    python3 -c "
import sys
src, dst, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
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
open(dst, 'wb').write(data + zigzag(count) + zigzag(0) + data[-16:])
" "$1" "$2" "$3"
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

# A negative declared object count: hasMore() tests != 0 and decr() only decrements.
avro_append_block "$DIR/ok.avro" "$DIR/negative.avro" -5
# A huge positive declared count: the loop terminates, but every row it reports past the payload is
# invented, so the count came back as a successful wrong answer.
avro_append_block "$DIR/ok.avro" "$DIR/huge.avro" 1000000000

echo '--- a corrupted block header is rejected instead of counted'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/negative.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'EOF reached'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/huge.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'EOF reached'
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
