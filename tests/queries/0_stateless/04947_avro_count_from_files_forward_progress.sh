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
# A 5th argument writes that many 0xff payload bytes before the sync marker.
avro_append_block() {
    python3 -c "
import sys
src, dst, count, bytes_declared = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
payload_len = int(sys.argv[5])
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
open(dst, 'wb').write(data + zigzag(count) + zigzag(bytes_declared) + b'\xff' * payload_len + data[-16:])
" "$1" "$2" "$3" "${4-0}" "${5-0}"
}

# Same, for a snappy-compressed file. An empty payload is rejected by the codec itself (a snappy
# block must carry at least the 4 checksum bytes), so the payload here is a real snappy stream of
# $4 literal bytes plus avro's own big-endian CRC-32 of those bytes: varint length, then a literal
# element whose tag byte is (len - 1) << 2.
avro_append_snappy_block() {
    python3 -c "
import sys, zlib
src, dst, count, literal_len = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            break
    return bytes(out)
raw = bytes(range(1, 1 + literal_len))
payload = varint(len(raw)) + bytes([(len(raw) - 1) << 2]) + raw + (zlib.crc32(raw) & 0xffffffff).to_bytes(4, 'big')
data = open(src, 'rb').read()
# Both header fields are non-negative here, so zigzag is just the value shifted left by one.
open(dst, 'wb').write(data + varint(count << 1) + varint(len(payload) << 1) + payload + data[-16:])
" "$1" "$2" "$3" "$4"
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
# snappy is what ClickHouse writes by default, and zstd is the fourth codec it can write.
for codec in snappy zstd; do
    $CLICKHOUSE_LOCAL -q "
        SELECT number AS n, toString(number) AS s FROM numbers(5000)
        INTO OUTFILE '$DIR/ok-$codec.avro' TRUNCATE FORMAT Avro
        SETTINGS output_format_avro_codec = '$codec'"
done
$CLICKHOUSE_LOCAL -q "
    SELECT NULL AS a FROM numbers(1000)
    INTO OUTFILE '$DIR/nullrows-snappy.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'snappy'"

# A negative declared object count. Never reaches zero by decrementing, so the count call used to
# spin here until the query deadline.
avro_append_block "$DIR/ok.avro" "$DIR/negative.avro" -5 0
# A negative declared byte count. Widens into an unbounded payload limit when cast unsigned.
avro_append_block "$DIR/ok.avro" "$DIR/negbytes.avro" 10 -1
# A positive declared count larger than the payload holds. The count must not be answered from the
# header alone, or every row reported past the payload is invented and returned as a success.
avro_append_block "$DIR/ok.avro" "$DIR/huge.avro" 1000000000 0
# The same inflated count on the three compressed codecs.
avro_append_snappy_block "$DIR/ok-snappy.avro" "$DIR/huge-snappy.avro" 1000000000 8
avro_append_block "$DIR/deflate.avro" "$DIR/huge-deflate.avro" 1000000000 0
avro_append_block "$DIR/ok-zstd.avro" "$DIR/huge-zstd.avro" 1000000000 0

# A block whose header is honest -- 2 objects do fit in 8 payload bytes for this schema -- but whose
# payload is 8 0xff bytes, an unterminated varint that no decoder can consume.
avro_append_block "$DIR/ok.avro" "$DIR/badpayload.avro" 2 8 8

# A fixture that failed to be written would turn every count below into a silent zero.
for f in ok nullrows deflate ok-snappy ok-zstd nullrows-snappy negative negbytes huge huge-snappy huge-deflate huge-zstd badpayload; do
    [ -s "$DIR/$f.avro" ] || echo "MISSING FIXTURE $f.avro"
done

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

# Name the size bound's own diagnostic: the broad AVRO_EXCEPTION grep above cannot tell it from
# payload exhaustion, from the snappy checksum, or from a snappy payload shorter than 4 bytes.
echo '--- a declared count the payload cannot hold is rejected at the header, not counted'
for setting in 1 0; do
    for f in huge.avro huge-snappy.avro; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count() FROM file('$DIR/$f', Avro)
            $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'in block header cannot fit in'
    done
done

# The bound is derived from the schema, not from the input size, so a file that legitimately holds
# more rows than it has payload bytes must still count. These are the files it could destroy.
echo '--- valid input on every codec counts the same with and without the count shortcut'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    for f in ok-snappy.avro ok-zstd.avro nullrows-snappy.avro; do
        $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/$f', Avro) $BOUND, optimize_count_from_files = $setting"
    done
done

# deflate and zstd decompress lazily, so the payload size is not known where the bound is applied
# (ClickHouse/avro c488095932fb). Only the read path rejects a corrupted header for those two.
echo '--- lazily decompressed codecs are rejected by the read path'
for f in huge-deflate.avro huge-zstd.avro; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/$f', Avro)
        $BOUND, optimize_count_from_files = 0" 2>&1 | grep -c -F 'AVRO_EXCEPTION'
done

# The shortcut answers from the block headers without decoding the payload, and that is its only
# user-visible difference from a full read. This file's headers are honest, so the size bound
# accepts it; its payload is not decodable, so only the reading path can fail on it.
echo '--- the count shortcut answers from the block header without decoding the payload'
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DIR/badpayload.avro', Avro)
    $BOUND, optimize_count_from_files = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DIR/badpayload.avro', Avro)
    $BOUND, optimize_count_from_files = 0" 2>&1 | grep -c -F 'AVRO_EXCEPTION'

rm -rf "$DIR"
