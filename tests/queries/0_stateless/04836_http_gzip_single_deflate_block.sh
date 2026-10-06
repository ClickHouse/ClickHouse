#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/114045: ingesting a gzip
# HTTP body whose DEFLATE payload is a single block spanning the whole stream - the shape
# zlib-ng's deflate_quick path (compression level 1, the default of the official .NET SDK)
# emits. The streaming decompressor used to re-decode the block from its start on every socket
# refill, making the ingest quadratic in the compressed size; a correct decoder handles this
# shape in linear time. No available encoder produces the shape on demand, so the gzip file is
# crafted directly: with the static Huffman code, every literal byte below 144 is an 8-bit
# codeword, so the block body is a byte translation of the payload shifted by the 3 header bits.
#
# The quadratic decoder produces the right bytes, it just does far more work, so the assertion is
# on work: the same payload is ingested twice - first as an ordinary multi-block gzip, which every
# decoder handles in linear time, then as the single block - and the single block must cost at most
# 10x the user CPU of the baseline. CPU, not elapsed time, because re-decoding is computation while
# an oversubscribed runner adds wall clock and no CPU: over eight CI runs the CPU ratio stayed in
# [1.005, 1.099] where wall clock spanned [0.41, 14.89], and a pre-fix binary sits at 122-127x.
# The payload is 90 lines of 400 KB rather than many short ones so that line parsing and the
# `MergeTree` write cost nothing next to the decompression under test (with 520000 short lines
# they dominated at 5.8 s). The multi-block baseline is ingested first so that any one-off warm-up
# cost lands on the baseline side.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MULTI_BLOCK_FILE=${CLICKHOUSE_TMP}/04836_multi_block.gz
SINGLE_BLOCK_FILE=${CLICKHOUSE_TMP}/04836_single_block.gz

python3 -c "
import hashlib, struct, sys, zlib
size, line = 36000000, 400000
chunk = b''.join(hashlib.sha256(b'%d' % i).hexdigest().encode() for i in range(size // 64 + 1))[:size]
raw = b'\n'.join(chunk[i:i + line] for i in range(0, size, line)) + b'\n'

# The baseline: an ordinary gzip stream of many DEFLATE blocks, as any regular encoder emits.
# Level 6 (not 1) so that the shape does not depend on whether Python links zlib or zlib-ng.
multi = zlib.compressobj(6, zlib.DEFLATED, 31)
open(sys.argv[1], 'wb').write(multi.compress(raw) + multi.flush())

# The shape under test: the whole payload as one static-Huffman DEFLATE block.
assert max(raw) < 144
table = bytes(int(format(0x30 + b, '08b')[::-1], 2) for b in range(144)) + bytes(112)
body = raw.translate(table)
# 3 header bits (BFINAL=1, BTYPE=01 static), 8 bits per literal, 7 zero bits of end-of-block.
n = (int.from_bytes(body, 'little') << 3) | 0b011
deflate = n.to_bytes(len(body) + 2, 'little')
blob = (b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff' + deflate
        + struct.pack('<II', zlib.crc32(raw), len(raw) % (1 << 32)))
assert zlib.decompress(blob, 31) == raw
open(sys.argv[2], 'wb').write(blob)
" "${MULTI_BLOCK_FILE}" "${SINGLE_BLOCK_FILE}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04836 (s String) ENGINE = MergeTree ORDER BY ()"

# Unique per invocation, not per database: the query_log reads below key on query_id alone.
suffix="${CLICKHOUSE_DATABASE}_$(random_str 10)"
qid_multi="04836_multi_${suffix}"
qid_single="04836_single_${suffix}"

function ingest()
{
    ${CLICKHOUSE_CURL} -sS -X POST -H 'Content-Encoding: gzip' -H 'Transfer-Encoding: chunked' \
        -T "$2" "${CLICKHOUSE_URL}&query_id=$1&query=INSERT%20INTO%20t_04836%20FORMAT%20LineAsString"
}

ingest "${qid_multi}" "${MULTI_BLOCK_FILE}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_04836"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE t_04836"

ingest "${qid_single}" "${SINGLE_BLOCK_FILE}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(length(s)), sum(cityHash64(s)) FROM t_04836"

# The query_log entry is written after the HTTP response is sent, so a single FLUSH LOGS races the
# log write (https://github.com/ClickHouse/ClickHouse/issues/84364).
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    landed=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(query_id = '${qid_multi}') = 1 AND countIf(query_id = '${qid_single}') = 1
        FROM system.query_log
        WHERE query_id IN ('${qid_multi}', '${qid_single}')
            AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()")
    [ "$landed" = "1" ] && break
    sleep 0.5
done

# An aggregate over an empty set still yields 0, so the arithmetic below always gets a number.
read -r MULTI_US SINGLE_US <<< "$(${CLICKHOUSE_CLIENT} --query "
    SELECT
        sumIf(ProfileEvents['UserTimeMicroseconds'], query_id = '${qid_multi}'),
        sumIf(ProfileEvents['UserTimeMicroseconds'], query_id = '${qid_single}')
    FROM system.query_log
    WHERE query_id IN ('${qid_multi}', '${qid_single}')
        AND type = 'QueryFinish' AND event_date >= yesterday() AND current_database = currentDatabase()")"

# Both counters must be nonzero, so that a measurement that went missing cannot read as success.
if (( MULTI_US == 0 || SINGLE_US == 0 || SINGLE_US > MULTI_US * 10 )); then
    echo "single-block ingest used ${SINGLE_US} us of CPU, multi-block ingest used ${MULTI_US} us"
fi
echo "single-block ingest within 10x of multi-block: $(( MULTI_US > 0 && SINGLE_US > 0 && SINGLE_US <= MULTI_US * 10 ))"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04836"
rm -f "${MULTI_BLOCK_FILE}" "${SINGLE_BLOCK_FILE}"
