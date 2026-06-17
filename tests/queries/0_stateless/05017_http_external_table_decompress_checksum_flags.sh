#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `<name>_decompress` and `<name>_disable_checksum` external-table HTTP parameters are
# booleans, just like the `decompress` / `compress` query parameters. An explicit `=0` must mean
# "off", not merely "the parameter is present". Previously they were presence-only, so
# `ext_decompress=0` still wrapped plain external data in a CompressedReadBuffer (failing an
# otherwise valid plain upload), and `ext_disable_checksum=0` still disabled checksum verification.

# 1. Plain (uncompressed) Native data with ext_decompress=0 must be read as-is, returning 100.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=0"

# Sanity: compressed data with ext_decompress=1 still decompresses, returning 100.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" | \
    ${CLICKHOUSE_CURL} -sSF 'ext=@-' "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1"

# 2. Corrupt the first byte of the compressed stream. It is part of the 16-byte block checksum
# (the compressed payload starts after the checksum, so the data itself stays intact), so the
# only observable effect is whether checksum verification is on.
DATA_FILE="${CLICKHOUSE_TMP}/05017_block.native"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+number+AS+id+FROM+numbers(100)+FORMAT+Native&compress=1" > "$DATA_FILE"
python3 -c "
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
b[0] ^= 0xff
open(p, 'wb').write(b)
" "$DATA_FILE"

# ext_disable_checksum=1: verification is off, the corrupted checksum is ignored and the intact
# payload decompresses, returning 100.
${CLICKHOUSE_CURL} -sSF 'ext=@'"$DATA_FILE" "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1&ext_disable_checksum=1"

# ext_disable_checksum=0: verification stays on, so the corrupted checksum is rejected (external
# data reports a checksum mismatch as CANNOT_DECOMPRESS).
${CLICKHOUSE_CURL} -sSF 'ext=@'"$DATA_FILE" "${CLICKHOUSE_URL}&query=SELECT+count()+FROM+ext&ext_structure=id+UInt64&ext_format=Native&ext_decompress=1&ext_disable_checksum=0" 2>&1 | \
    grep -o -m1 'CANNOT_DECOMPRESS'

rm -f "$DATA_FILE"
