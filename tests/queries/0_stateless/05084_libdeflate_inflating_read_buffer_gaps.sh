#!/usr/bin/env bash
# Tags: no-fasttest

# LibdeflateInflatingReadBuffer (src/IO/LibdeflateInflatingReadBuffer.cpp)
# handles three optional gzip header extension fields defined by RFC 1952.
# All three code paths were previously unexercised in CI because standard
# compression tools (gzip, zstd, etc.) never emit these fields and no test
# crafted a binary gzip payload that set the corresponding FLG bits.
#
#   FEXTRA  (FLG bit 2) — lines 144-155
#     An optional "extra" field: a 2-byte little-endian XLEN followed by
#     XLEN bytes of application-defined data.  The decompressor must skip
#     this block before reading the file name and comment fields.
#     A regression here would cause ClickHouse to misparse the gzip header
#     and produce garbage data or a CANNOT_DECOMPRESS error on valid files
#     written by tools that include extension metadata (e.g. some archiving
#     utilities set SI1/SI2 sub-fields for OS metadata).
#
#   FCOMMENT (FLG bit 4) — lines 168-179
#     An optional NUL-terminated comment string.  The decompressor scans
#     bytes until it finds the NUL terminator.  A regression here would
#     stall or misparse on any gzip file carrying an embedded comment.
#
#   FHCRC   (FLG bit 1) — line 188 (success path)
#     A 2-byte CRC16 of all preceding header bytes.  CI had one test that
#     exercised the mismatch branch (line 186, always throws), but the
#     success path (line 188, in_pos += 2) was never executed.  A regression
#     here would reject valid FHCRC-protected gzip files or skip the CRC
#     check silently.
#
# Each sub-test reads a hand-crafted binary .csv.gz file that sets exactly
# the target FLG bit, with the payload being a single integer value.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

SERVER_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"
SERVER_ABS="${USER_FILES_PATH}/${SERVER_REL}"
mkdir -p "${SERVER_ABS}"

trap 'rm -rf "${SERVER_ABS}"' EXIT

# Craft three binary gzip files with different optional header extensions.
python3 - "${SERVER_ABS}" <<'PYEOF'
import struct, sys, zlib

dst = sys.argv[1]

def deflate_raw(data):
    """Raw DEFLATE stream: zlib.compress output minus 2-byte header and 4-byte Adler-32 trailer."""
    return zlib.compress(data)[2:-4]

def gzip_footer(data):
    """gzip footer: CRC32 of uncompressed data + little-endian 32-bit size."""
    return struct.pack('<II', zlib.crc32(data) & 0xFFFFFFFF, len(data) & 0xFFFFFFFF)

# Fixed gzip header fields: ID1 ID2 CM FLG   MTIME(4)   XFL OS
def base_header(flg):
    return bytes([0x1f, 0x8b, 0x08, flg]) + b'\x00' * 4 + b'\x00\x03'

# --- FEXTRA (FLG bit 2): RFC 1952 §2.3.1.1 ---
# The extra block starts with a 2-byte little-endian XLEN, then XLEN bytes.
extra_data = b'\x41\x42\x00\x02\xde\xad'   # 6 arbitrary bytes
flg = 0x04                                   # FEXTRA only
data = b'3\n'
header = base_header(flg) + struct.pack('<H', len(extra_data)) + extra_data
with open(f'{dst}/fextra.csv.gz', 'wb') as f:
    f.write(header + deflate_raw(data) + gzip_footer(data))

# --- FCOMMENT (FLG bit 4): NUL-terminated comment ---
comment = b'test comment\x00'
flg = 0x10                                   # FCOMMENT only
data = b'4\n'
header = base_header(flg) + comment
with open(f'{dst}/fcomment.csv.gz', 'wb') as f:
    f.write(header + deflate_raw(data) + gzip_footer(data))

# --- FHCRC (FLG bit 1): CRC16 of all preceding header bytes ---
flg = 0x02                                   # FHCRC only
data = b'5\n'
hdr = base_header(flg)
crc16 = zlib.crc32(hdr) & 0xFFFF            # low 16 bits of CRC32 over header bytes
header = hdr + struct.pack('<H', crc16)
with open(f'{dst}/fhcrc.csv.gz', 'wb') as f:
    f.write(header + deflate_raw(data) + gzip_footer(data))
PYEOF

# --- Test 1: GZIP_FEXTRA path (LibdeflateInflatingReadBuffer.cpp:144-155) ---
echo "fextra_value:"
${CLICKHOUSE_CLIENT} -q \
    "SELECT x FROM file('${SERVER_REL}/fextra.csv.gz', 'CSV', 'x UInt8')"

# --- Test 2: GZIP_FCOMMENT path (LibdeflateInflatingReadBuffer.cpp:168-179) ---
echo "fcomment_value:"
${CLICKHOUSE_CLIENT} -q \
    "SELECT x FROM file('${SERVER_REL}/fcomment.csv.gz', 'CSV', 'x UInt8')"

# --- Test 3: GZIP_FHCRC success path (LibdeflateInflatingReadBuffer.cpp:188) ---
echo "fhcrc_value:"
${CLICKHOUSE_CLIENT} -q \
    "SELECT x FROM file('${SERVER_REL}/fhcrc.csv.gz', 'CSV', 'x UInt8')"
