#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A belt so a regression fails fast instead of hanging the suite, never an assertion in itself.
BOUND="SETTINGS max_execution_time = 30"

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

# Append a well-framed block (zigzag object count, zigzag byte count, $5 bytes of 0xff payload, the
# file's own sync marker), so only the declared counts are wrong.
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

# Same for snappy, whose payload must be a real snappy stream: a varint length, a literal element
# tagged (len - 1) << 2, then avro's big-endian CRC-32. A 5th argument fills it with that byte, which
# does not decode as rows; without it the payload is 1, 2, 3, ... and does.
avro_append_snappy_block() {
    python3 -c "
import sys, zlib
src, dst, count, literal_len = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
fill = int(sys.argv[5])
def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7f
        n >>= 7
        out.append(b | 0x80 if n else b)
        if not n:
            break
    return bytes(out)
raw = bytes([fill]) * literal_len if fill >= 0 else bytes(range(1, 1 + literal_len))
payload = varint(len(raw)) + bytes([(len(raw) - 1) << 2]) + raw + (zlib.crc32(raw) & 0xffffffff).to_bytes(4, 'big')
data = open(src, 'rb').read()
# Both header fields are non-negative here, so zigzag is just the value shifted left by one.
open(dst, 'wb').write(data + varint(count << 1) + varint(len(payload) << 1) + payload + data[-16:])
" "$1" "$2" "$3" "$4" "${5--1}"
}

# Keep this small: the block-size sweep below reads ok.avro one row per chunk, and every chunk is a
# pipeline handoff the flaky check's ThreadFuzzer can sleep at.
ROWS=500

# An all-NULL column encodes to zero payload bytes, so nullrows.avro legitimately declares 1000 rows
# in a few hundred bytes and deflate.avro 200000 in under a kilobyte.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toString(number) AS s FROM numbers($ROWS)
    INTO OUTFILE '$DIR/ok.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'null';
    SELECT NULL AS a FROM numbers(1000)
    INTO OUTFILE '$DIR/nullrows.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'null';
    SELECT 1 AS a FROM numbers(200000)
    INTO OUTFILE '$DIR/deflate.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'deflate';
    SELECT number AS n, toString(number) AS s FROM numbers($ROWS)
    INTO OUTFILE '$DIR/ok-snappy.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'snappy';
    SELECT number AS n, toString(number) AS s FROM numbers($ROWS)
    INTO OUTFILE '$DIR/ok-zstd.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'zstd';
    SELECT NULL AS a FROM numbers(1000)
    INTO OUTFILE '$DIR/nullrows-snappy.avro' TRUNCATE FORMAT Avro
    SETTINGS output_format_avro_codec = 'snappy'"

# A negative object count never reaches zero by decrementing, so the count call used to spin here
# until the query deadline; a negative byte count widens into an unbounded payload limit when cast
# unsigned. Then the same inflated count on each codec.
avro_append_block "$DIR/ok.avro" "$DIR/negative.avro" -5 0
avro_append_block "$DIR/ok.avro" "$DIR/negbytes.avro" 10 -1
avro_append_block "$DIR/ok.avro" "$DIR/huge.avro" 1000000000 0
avro_append_snappy_block "$DIR/ok-snappy.avro" "$DIR/huge-snappy.avro" 1000000000 8
avro_append_block "$DIR/deflate.avro" "$DIR/huge-deflate.avro" 1000000000 0
avro_append_block "$DIR/ok-zstd.avro" "$DIR/huge-zstd.avro" 1000000000 0

# An honest header -- 2 objects do fit in 8 payload bytes for this schema -- over a payload no
# decoder can consume, so only the read path can fail on it.
avro_append_block "$DIR/ok.avro" "$DIR/badpayload.avro" 2 8 8
avro_append_snappy_block "$DIR/ok-snappy.avro" "$DIR/badpayload-snappy.avro" 2 8 255

# Both fields inflated together, which passes a fit check that compares them to each other.
avro_append_block "$DIR/ok.avro" "$DIR/inflated-both.avro" 1000000000 2000000000

for f in ok nullrows deflate ok-snappy ok-zstd nullrows-snappy negative negbytes huge huge-snappy huge-deflate huge-zstd badpayload badpayload-snappy inflated-both; do
    [ -s "$DIR/$f.avro" ] || echo "MISSING FIXTURE $f.avro"
done

# A corrupted block also runs out of input, so these arms grep the header diagnostic rather than the
# error class, which cannot tell the header check from payload exhaustion or from a snappy checksum.
echo '--- a negative declared count is rejected at the header, not at the payload'
for setting in 1 0; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/negative.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'object count in block header'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/negbytes.avro', Avro)
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'byte count in block header'
done

# The bound is derived from the schema, not from the input size, so a file that legitimately holds
# more rows than it has payload bytes must still count, on every codec ClickHouse writes.
echo '--- valid input counts the same with and without the count shortcut'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/ok.avro', Avro) $BOUND, optimize_count_from_files = $setting;
        SELECT count() FROM file('$DIR/nullrows.avro', Avro) $BOUND, optimize_count_from_files = $setting;
        SELECT count() FROM file('$DIR/deflate.avro', Avro) $BOUND, optimize_count_from_files = $setting;
        SELECT count() FROM file('$DIR/ok-snappy.avro', Avro) $BOUND, optimize_count_from_files = $setting;
        SELECT count() FROM file('$DIR/ok-zstd.avro', Avro) $BOUND, optimize_count_from_files = $setting;
        SELECT count() FROM file('$DIR/nullrows-snappy.avro', Avro) $BOUND, optimize_count_from_files = $setting"
done

echo '--- reading valid input is unchanged, at several block sizes'
$CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(cityHash64(n, s)) FROM file('$DIR/ok.avro', Avro) $BOUND, max_block_size = 1;
    SELECT count(), sum(cityHash64(n, s)) FROM file('$DIR/ok.avro', Avro) $BOUND, max_block_size = 13;
    SELECT count(), sum(cityHash64(n, s)) FROM file('$DIR/ok.avro', Avro) $BOUND, max_block_size = 65505"

echo '--- a declared count the payload cannot hold is rejected at the header, not counted'
for setting in 1 0; do
    for f in huge.avro huge-snappy.avro; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count() FROM file('$DIR/$f', Avro)
            $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'in block header cannot fit in'
    done
done

# Answering from the block headers without decoding the payload is the shortcut's only user-visible
# difference from a full read, so this file's honest headers count while its payload cannot be read.
echo '--- the count shortcut answers from the block header without decoding the payload'
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DIR/badpayload-snappy.avro', Avro)
    $BOUND, optimize_count_from_files = 1"
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$DIR/badpayload-snappy.avro', Avro)
    $BOUND, optimize_count_from_files = 0" 2>&1 | grep -c -F 'AVRO_EXCEPTION'

# Where the declared payload size is never materialized the shortcut must be declined and both
# settings reach the read path: null only compares two fields of the same corrupted header, which
# inflated-both passes, and deflate and zstd decompress lazily (ClickHouse/avro c488095932fb).
echo '--- the shortcut is declined where the declared payload size is not verified'
for f in inflated-both.avro badpayload.avro huge-deflate.avro huge-zstd.avro; do
    for setting in 1 0; do
        $CLICKHOUSE_LOCAL -q "
            SELECT count() FROM file('$DIR/$f', Avro)
            $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'AVRO_EXCEPTION'
    done
done

rm -rf "$DIR"
