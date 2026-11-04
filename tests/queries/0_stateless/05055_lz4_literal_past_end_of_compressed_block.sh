#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A compressed block is `[checksum: 16][method: 1][size_compressed: 4][size_decompressed: 4][payload]`,
# and the LZ4 decoder is given 64 bytes of slack after the payload for its unconditional wild copies.
# Those slack bytes are not part of the payload, so a block whose last literal declares more bytes than
# the payload carries has to be rejected instead of filling the decompressed block from the slack.

# A well-formed block: a single 200-byte literal of `0xaa` that ends exactly at the end of the payload.
PRIME="db91261aaac942286f3030571640ef2682d3000000c8000000f0b9$(printf 'aa%.0s' {1..200})"

# A crafted block: `size_decompressed` is 40 and the literal declares 40 bytes, but only 8 are supplied.
# Before the fix the remaining 32 bytes were copied out of the slack, which is the tail of the previously
# read block (in a server, arbitrary heap memory), and the decoder reported success.
EVIL="0ef0349f888764523cefcf446b85dee6821300000028000000f0194141414141414141"

echo -n "$PRIME" | xxd -r -p > "${CLICKHOUSE_TMP}/05055_prime.compressed"
echo -n "$EVIL" | xxd -r -p > "${CLICKHOUSE_TMP}/05055_evil.compressed"
cat "${CLICKHOUSE_TMP}/05055_prime.compressed" "${CLICKHOUSE_TMP}/05055_evil.compressed" > "${CLICKHOUSE_TMP}/05055_both.compressed"

echo -n 'well-formed block: '
$CLICKHOUSE_COMPRESSOR --decompress --input "${CLICKHOUSE_TMP}/05055_prime.compressed" --output "${CLICKHOUSE_TMP}/05055_prime.decompressed" 2>&1
wc -c < "${CLICKHOUSE_TMP}/05055_prime.decompressed" | tr -d ' '

echo -n 'literal past the end of the payload: '
$CLICKHOUSE_COMPRESSOR --decompress --input "${CLICKHOUSE_TMP}/05055_evil.compressed" --output "${CLICKHOUSE_TMP}/05055_evil.decompressed" 2>&1 | grep -o -m1 'CANNOT_DECOMPRESS'

echo -n 'the same, after a block that primes the slack: '
$CLICKHOUSE_COMPRESSOR --decompress --input "${CLICKHOUSE_TMP}/05055_both.compressed" --output "${CLICKHOUSE_TMP}/05055_both.decompressed" 2>&1 | grep -o -m1 'CANNOT_DECOMPRESS'

rm -f "${CLICKHOUSE_TMP}"/05055_*.compressed "${CLICKHOUSE_TMP}"/05055_*.decompressed
