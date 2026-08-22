#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro, MsgPack and Protobuf formats are not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# max_execution_time is a safety belt so a regression fails fast instead of hanging the suite. The
# assertion is always the error code or the row count, never the elapsed time.
BOUND="SETTINGS max_execution_time = 30"

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

PB_STRUCT="a UInt64, b Int32, c Float64, d String"

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
    SELECT number AS a, toInt32(number) AS b, toFloat64(number) AS c, toString(number) AS d
    FROM numbers(777)
    INTO OUTFILE '$DIR/ok.pb' TRUNCATE FORMAT ProtobufList"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS c1, number * 2 AS c2 FROM numbers(555)
    INTO OUTFILE '$DIR/ok.msgpk' TRUNCATE FORMAT MsgPack"
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

# A ProtobufList envelope whose payload ends mid-message: readFieldNumber reports the end of the
# message while the underlying buffer still has bytes, so the count loop made no progress.
printf '\x00\x00' > "$DIR/bad.pb"
# 0xc1 is the one byte value MessagePack never assigns, so the parser reports a parse error without
# consuming it.
printf '\xc1' > "$DIR/bad.msgpk"
# Three well formed objects read back with a two column schema, so the last row is missing its second
# value: the object count is not a multiple of the column count.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS c1 FROM numbers(3)
    INTO OUTFILE '$DIR/odd.msgpk' TRUNCATE FORMAT MsgPack"
# A negative declared object count: hasMore() tests != 0 and decr() only decrements.
avro_append_block "$DIR/ok.avro" "$DIR/negative.avro" -5
# A huge positive declared count: the loop terminates, but every row it reports past the payload is
# invented, so the count came back as a successful wrong answer.
avro_append_block "$DIR/ok.avro" "$DIR/huge.avro" 1000000000

echo '--- corrupted input is rejected instead of counted'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/bad.pb', ProtobufList, '$PB_STRUCT')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Unexpected end of ProtobufList message'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/bad.msgpk', MsgPack, 'c1 UInt64')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Error occurred while parsing msgpack data'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/odd.msgpk', MsgPack, 'c1 UInt64, c2 UInt64')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Not enough values to complete the row'
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
    for args in "'$DIR/ok.pb', ProtobufList, '$PB_STRUCT'" \
                "'$DIR/ok.msgpk', MsgPack, 'c1 UInt64, c2 UInt64'" \
                "'$DIR/ok.avro', Avro" \
                "'$DIR/nullrows.avro', Avro" \
                "'$DIR/deflate.avro', Avro"; do
        $CLICKHOUSE_LOCAL -q "SELECT count() FROM file($args) $BOUND, optimize_count_from_files = $setting"
    done
done

echo '--- an empty and a single byte ProtobufList input are still valid and empty'
: > "$DIR/empty.pb"
printf '\x00' > "$DIR/one.pb"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/empty.pb', ProtobufList, '$PB_STRUCT') $BOUND"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/one.pb', ProtobufList, '$PB_STRUCT') $BOUND"

echo '--- reading valid input is unchanged, at several block sizes'
for size in 1 13 65505; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(a, b, c, d))
        FROM file('$DIR/ok.pb', ProtobufList, '$PB_STRUCT')
        $BOUND, max_block_size = $size"
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(c1, c2))
        FROM file('$DIR/ok.msgpk', MsgPack, 'c1 UInt64, c2 UInt64')
        $BOUND, max_block_size = $size"
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(n, s))
        FROM file('$DIR/ok.avro', Avro)
        $BOUND, max_block_size = $size"
done

rm -rf "$DIR"
