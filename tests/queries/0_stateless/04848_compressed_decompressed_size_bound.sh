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
emit() { # $1 = frame body as hex, checksum excluded
    local checksum
    checksum=$(python3 -c "
import sys
b = bytearray.fromhex(sys.argv[1]); b[0] ^= 1
sys.stdout.buffer.write(bytes(b))
" "$1" | $CLICKHOUSE_BINARY checksum-for-compressed-block | awk -F'\t' '$2 == "0, 0" { print $1; exit }')
    python3 -c "
import sys
sys.stdout.buffer.write(bytearray.fromhex(sys.argv[1])[::-1] + bytearray.fromhex(sys.argv[2]))
" "$checksum" "$1"
}

# Method bytes are from CompressionInfo.h: NONE is 0x02 = 2 and Quantized is 0x9e = 158.
frame() { # $1 = method byte (decimal), $2 = size_decompressed, $3 = payload
    emit "$(python3 -c "
import struct, sys
payload = sys.argv[3].encode()
sys.stdout.buffer.write(bytes([int(sys.argv[1])]) + struct.pack('<I', 9 + len(payload)) + struct.pack('<I', int(sys.argv[2])) + payload)
" "$1" "$2" "$3" | xxd -p | tr -d '\n')"
}

post() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&decompress=1" --data-binary @-; }

echo '-- a valid frame still executes (proves the arms below fail for the intended reason)'
frame 2 8 'SELECT 1' | post

echo '-- a codec that stores data uncompressed must not lie about the uncompressed size'
frame 2 999 'SELECT 1' | post 2>&1 | grep -c '(8) does not match size_decompressed (999)'
# Quantized is the other codec reporting isNone(), so it takes the same shortcut. The read path
# builds it from the method byte alone, without the enable_quantized_codec setting its DDL requires.
echo '-- and neither may the other verbatim codec'
frame 158 999 'SELECT 1' | post 2>&1 | grep -c '(8) does not match size_decompressed (999)'

# The comparison is for inequality, so arms that only ever declare more than the body pin one side of
# it: narrowed to "body shorter than declared", both frames above would still be refused. This one
# declares less instead, and without the check it is not refused at all, it executes its real body.
echo '-- nor may it understate the uncompressed size'
frame 2 3 'SELECT 1' | post 2>&1 | grep -c '(8) does not match size_decompressed (3)'

# No-regression control: the shortcut is the ordinary read path for compressed marks and for the Log
# family, so engines reading frames they wrote themselves have to keep working.
echo '-- engines keep working on ordinary data'
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_log_bound;
    DROP TABLE IF EXISTS t_stripe_bound;
    DROP TABLE IF EXISTS t_mt_bound;

    CREATE TABLE t_log_bound (s String) ENGINE = Log;
    CREATE TABLE t_stripe_bound (s String) ENGINE = StripeLog;
    CREATE TABLE t_mt_bound (k UInt64, s String, INDEX idx_s s TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8, compress_marks = 1, compress_primary_key = 1,
                 min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;

    INSERT INTO t_log_bound SELECT repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_stripe_bound SELECT repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_mt_bound SELECT number, repeat('a', 100) FROM numbers(1000);

    SELECT count(), sum(length(s)) FROM t_log_bound;
    SELECT count(), sum(length(s)) FROM t_stripe_bound;
    SELECT count(), sum(length(s)) FROM t_mt_bound WHERE s LIKE '%a%';
    SELECT count() FROM t_mt_bound WHERE k = 42;
    -- packed_skip_index_max_bytes = 0 keeps the index in its own file, which is the skip-index class
    -- checkDataPart iterates over; a packed one carries no per-file checksum entry to visit.
    CHECK TABLE t_mt_bound SETTINGS check_query_single_value_result = 1;

    DROP TABLE t_log_bound;
    DROP TABLE t_stripe_bound;
    DROP TABLE t_mt_bound;
"
