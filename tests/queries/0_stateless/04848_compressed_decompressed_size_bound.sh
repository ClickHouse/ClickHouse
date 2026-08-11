#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o pipefail

# Wire layout of a compressed frame:
#   [16B checksum][1B method][4B LE size_compressed][4B LE size_decompressed][payload]
# size_compressed counts the 9-byte header. The checksum is an unkeyed CityHash128 of everything
# after it, so a peer can compute it: these frames are accepted at default settings, and
# http_native_compression_disable_checksumming_on_decompress is deliberately NOT used.
#
# checksum-for-compressed-block prints CityHash128 of every single-bit mutation of its input, so
# feeding it the body with bit 0 flipped yields CityHash128(body) on the line labelled "0, 0". The
# wire order is low64 then high64, each little-endian, i.e. the reverse of the printed hex.
frame() { # $1 = method byte (decimal), $2 = size_decompressed, $3 = payload
    local body checksum
    body=$(python3 -c "
import struct, sys
payload = sys.argv[3].encode()
sys.stdout.buffer.write(bytes([int(sys.argv[1])]) + struct.pack('<I', 9 + len(payload)) + struct.pack('<I', int(sys.argv[2])) + payload)
" "$1" "$2" "$3" | xxd -p | tr -d '\n')
    checksum=$(python3 -c "
import sys
b = bytearray.fromhex(sys.argv[1]); b[0] ^= 1
sys.stdout.buffer.write(bytes(b))
" "$body" | $CLICKHOUSE_BINARY checksum-for-compressed-block | awk -F'\t' '$2 == "0, 0" { print $1; exit }')
    python3 -c "
import sys
sys.stdout.buffer.write(bytearray.fromhex(sys.argv[1])[::-1] + bytearray.fromhex(sys.argv[2]))
" "$checksum" "$body"
}

post() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&decompress=1" --data-binary @-; }

# LZ4 is method 0x82 = 130, NONE is 0x02 = 2.

echo '-- a valid frame still executes (proves the arms below fail for the intended reason)'
frame 2 8 'SELECT 1' | post

echo '-- size_decompressed of 2 GiB is rejected from the header alone, before any allocation'
frame 130 2147483648 'SELECT 1' | post 2>&1 | grep -c 'Too large size_decompressed: 2147483648'

echo '-- the bound is inclusive: exactly 1 GiB is not rejected by it (it fails later, in decoding)'
frame 130 1073741824 'SELECT 1' | post 2>&1 | grep -c 'Too large size_decompressed'
echo '-- one byte above 1 GiB is rejected'
frame 130 1073741825 'SELECT 1' | post 2>&1 | grep -c 'Too large size_decompressed: 1073741825'

echo '-- a codec that stores data uncompressed must not lie about the uncompressed size'
frame 2 999 'SELECT 1' | post 2>&1 | grep -c 'does not match size_decompressed (999)'

# compressor --stat prints: codec, size_decompressed, size_compressed. Both fields are bounded, and
# only $3 sees the codec's expansion, so asserting $2 alone cannot observe an over-bound frame.
# --stat also refuses a frame above the bound outright, which produces no rows at all: an empty
# read must be reported, not counted as zero over-bound frames.
over_bound() {
    ${CLICKHOUSE_BINARY} compressor --stat --input "$1" 2>&1 \
        | awk -F'\t' '
            /^Code: 39/ { print "frames above the bound: rejected by --stat"; bad = 1; exit }
            { seen++; if ($2 > 1073741824 || $3 > 1073741824) over++ }
            END { if (bad) exit; if (!seen) print "frames above the bound: no frames read";
                  else print "frames above the bound:", over + 0 }'
}

echo '-- a writer asked for a frame above the bound emits several frames within it'
${CLICKHOUSE_BINARY} compressor --block-size 1207959552 < <(head -c 1207959552 /dev/zero) \
    > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped.bin"
over_bound "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped.bin"

echo '-- the same holds for the parallel writer'
${CLICKHOUSE_BINARY} compressor --threads 2 --block-size 1207959552 < <(head -c 1207959552 /dev/zero) \
    > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped_parallel.bin"
over_bound "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped_parallel.bin"

echo '-- a codec that does not compress expands the frame by the header alone, and still fits'
${CLICKHOUSE_BINARY} compressor --none --block-size 1073741824 < <(head -c 1073741824 /dev/zero) \
    > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_none.bin"
over_bound "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_none.bin"

echo '-- and so does a compressing codec handed incompressible input'
head -c 1073741824 /dev/urandom > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_rand.raw"
${CLICKHOUSE_BINARY} compressor --block-size 1073741824 \
    < "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_rand.raw" \
    > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_incompressible.bin"
over_bound "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_incompressible.bin"

echo '-- the capped output round-trips'
${CLICKHOUSE_BINARY} compressor --decompress --input "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped.bin" \
    | wc -c
${CLICKHOUSE_BINARY} compressor --decompress --input "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_incompressible.bin" \
    | cmp -s - "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_rand.raw" && echo 'incompressible input round-trips'

rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped.bin" "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_capped_parallel.bin" \
      "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_none.bin" "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_incompressible.bin" \
      "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_rand.raw"

echo '-- a reader of data an uncapped writer produced accepts an over-bound frame'
frame 130 2147483648 'SELECT 1' > "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_over.bin"
${CLICKHOUSE_BINARY} compressor --decompress --input "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_over.bin" \
    --output /dev/null 2>&1 | grep -c 'Too large size_decompressed'
echo '-- and so does its seeking path'
${CLICKHOUSE_BINARY} compressor --decompress --offset-in-decompressed-block 1 \
    --input "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_over.bin" --output /dev/null 2>&1 \
    | grep -c 'Too large size_decompressed'
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_over.bin"

# No-regression control: it asserts that engines reading their own files keep working, and it
# passes with every reader opt-out removed. The opt-outs are covered by the two
# 'compressor --decompress' arms above, which redden without them. A genuine over-bound engine
# file cannot be produced here, because the writer cap prevents one and fabricating part bytes is
# not allowed in a stateless test.
echo '-- engines whose readers accept such frames keep working on ordinary data'
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_log_bound;
    DROP TABLE IF EXISTS t_stripe_bound;
    DROP TABLE IF EXISTS t_mt_bound;

    CREATE TABLE t_log_bound (s String) ENGINE = Log;
    CREATE TABLE t_stripe_bound (s String) ENGINE = StripeLog;
    CREATE TABLE t_mt_bound (k UInt64, s String, INDEX idx_s s TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8, compress_marks = 1, compress_primary_key = 1,
                 min_bytes_for_wide_part = 0;

    INSERT INTO t_log_bound SELECT repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_stripe_bound SELECT repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_mt_bound SELECT number, repeat('a', 100) FROM numbers(1000);

    SELECT count(), sum(length(s)) FROM t_log_bound;
    SELECT count(), sum(length(s)) FROM t_stripe_bound;
    SELECT count(), sum(length(s)) FROM t_mt_bound WHERE s LIKE '%a%';
    SELECT count() FROM t_mt_bound WHERE k = 42;
    -- Reaches the skip-index file class in checkDataPart, which a read also reaches.
    CHECK TABLE t_mt_bound SETTINGS check_query_single_value_result = 1;

    DROP TABLE t_log_bound;
    DROP TABLE t_stripe_bound;
    DROP TABLE t_mt_bound;
"
