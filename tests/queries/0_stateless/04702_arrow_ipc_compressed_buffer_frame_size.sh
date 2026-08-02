#!/usr/bin/env bash
# Tags: no-fasttest
# Regression tests for malformed per-buffer uncompressed-length prefixes in a compressed Arrow IPC
# RecordBatch body. The corruption is in the RecordBatch body, not the Schema message, so only a
# data read reaches it: schema inference must still succeed. Each case must be rejected as
# INCORRECT_DATA rather than allocating for the declared size:
#   - a payload that is not a parsable codec frame;
#   - a prefix that disagrees with the size its own codec frame declares;
#   - an accumulated body size the allocator would reject as an internal error (LOGICAL_ERROR).
# A prefix that agrees with its frame on a large size is NOT corrupt: it stays a memory-limit
# condition, which the last case asserts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# Two writers, because the frame's content size is optional and each case needs one of the two:
#   - ClickHouse sets it, so a prefix can be patched to disagree with it (cases 2 and 4);
#   - pyarrow omits it, so a prefix can be patched without any frame size to contradict (cases 1, 3),
#     which is also the case the frame/prefix comparison must SKIP rather than reject.
$CLICKHOUSE_LOCAL --query "
    SELECT number AS i, toString(number) AS s, number * 0.5 AS f FROM numbers(4000)
    INTO OUTFILE '${TMP_DIR}/ch_lz4.arrows' TRUNCATE FORMAT ArrowStream
    SETTINGS output_format_arrow_compression_method = 'lz4_frame'"

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
LZ4_MAGIC = b"\x04\x22\x4d\x18"

# Several columns, so the batch carries several compressed buffers: the accumulated-total case needs
# at least two, and a String column contributes both an offsets and a values buffer.
tbl = pa.table({
    "i": pa.array(list(range(4000)), pa.int64()),
    "s": pa.array(["row%d" % i for i in range(4000)]),
    "f": pa.array([i * 0.5 for i in range(4000)], pa.float64()),
})

for codec in ("lz4", "zstd"):
    with pa.OSFile(f"{out}/wellformed_{codec}.arrows", "wb") as sink:
        with ipc.new_stream(sink, tbl.schema, options=ipc.IpcWriteOptions(compression=codec)) as w:
            w.write_table(tbl)

arrow_lz4 = bytearray(open(f"{out}/wellformed_lz4.arrows", "rb").read())
ch_lz4 = bytearray(open(f"{out}/ch_lz4.arrows", "rb").read())


def prefix_offsets(data):
    """Offsets of the 8-byte uncompressed-length prefixes, found via the LZ4 frame magic."""
    res, i = [], 0
    while True:
        i = data.find(LZ4_MAGIC, i)
        if i < 0:
            return res
        if i >= 8:
            res.append(i - 8)
        i += 4


def xxh32(data, seed=0):
    """The LZ4 frame header checksum, which must be recomputed after patching the header. Pure
    python so the test needs no module beyond pyarrow."""
    P1, P2, P3, P4, P5 = 2654435761, 2246822519, 3266489917, 668265263, 374761393
    M = 0xFFFFFFFF

    def rol(x, r):
        return ((x << r) | (x >> (32 - r))) & M

    n, idx = len(data), 0
    if n >= 16:
        v = [(seed + P1 + P2) & M, (seed + P2) & M, seed & M, (seed - P1) & M]
        while idx + 16 <= n:
            for k in range(4):
                lane, = struct.unpack_from("<I", data, idx + 4 * k)
                v[k] = (rol((v[k] + lane * P2) & M, 13) * P1) & M
            idx += 16
        h = (rol(v[0], 1) + rol(v[1], 7) + rol(v[2], 12) + rol(v[3], 18)) & M
    else:
        h = (seed + P5) & M
    h = (h + n) & M
    while idx + 4 <= n:
        lane, = struct.unpack_from("<I", data, idx)
        h = (rol((h + lane * P3) & M, 17) * P4) & M
        idx += 4
    while idx < n:
        h = (rol((h + data[idx] * P5) & M, 11) * P1) & M
        idx += 1
    h ^= h >> 15
    h = (h * P2) & M
    h ^= h >> 13
    h = (h * P3) & M
    h ^= h >> 16
    return h


def set_frame_content_size(d, prefix_off, value):
    """Patch the frame's contentSize in place, then fix the header checksum.

    Frame header: magic(4) FLG(1) BD(1) [contentSize(8) if FLG&0x08] [dictID(4) if FLG&0x01] HC(1).
    In place, so the buffer layout after this frame is left untouched.
    """
    flg_off = prefix_off + 12
    flg = d[flg_off]
    assert flg & 0x08, "writer did not record a frame content size"
    assert not (flg & 0x01), "dictID present, offsets would shift"
    struct.pack_into("<Q", d, prefix_off + 14, value)
    hc_off = prefix_off + 22
    d[hc_off] = (xxh32(bytes(d[flg_off:hc_off])) >> 8) & 0xFF


ch_offs = prefix_offsets(ch_lz4)
arrow_offs = prefix_offsets(arrow_lz4)
assert len(arrow_offs) >= 2, f"expected >= 2 compressed buffers, got {len(arrow_offs)}"

# Case 1: not a parsable frame. Clear the magic of the first buffer's payload.
d = bytearray(arrow_lz4)
d[arrow_offs[0] + 8:arrow_offs[0] + 12] = b"\x00\x00\x00\x00"
open(f"{out}/bad_frame.arrows", "wb").write(bytes(d))

# Case 2: the prefix disagrees with the size its own frame declares. Far larger than the real size,
# but below the allocator's ceiling, so it is the frame comparison and not the total that rejects it.
d = bytearray(ch_lz4)
struct.pack_into("<q", d, ch_offs[0], 100 * 1024 ** 3)
open(f"{out}/prefix_mismatch.arrows", "wb").write(bytes(d))

# Case 3: two prefixes summing just past 2^62, in frames that declare no size so the comparison is
# skipped. `PODArray::resize` rounds up to a power of two, so anything above 2^62 would reach the
# allocator's 2^63 ceiling; the accumulated total is what must be rejected here.
d = bytearray(arrow_lz4)
half = (1 << 61) + 8
struct.pack_into("<q", d, arrow_offs[0], half)
struct.pack_into("<q", d, arrow_offs[1], half)
open(f"{out}/aggregate_too_large.arrows", "wb").write(bytes(d))

# Case 4 (NOT corrupt): the prefix and the frame agree on a large size. Patch both in place.
d = bytearray(ch_lz4)
big = 8 * 1024 ** 3
struct.pack_into("<q", d, ch_offs[0], big)
set_frame_content_size(d, ch_offs[0], big)
open(f"{out}/consistent_large.arrows", "wb").write(bytes(d))
PYEOF

check() {
    $CLICKHOUSE_LOCAL --max_memory_usage=1G \
        --query "SELECT * FROM file('${TMP_DIR}/$1', ArrowStream) FORMAT Null" 2>&1 \
        | grep -F -q "$2" && echo "OK $1" || echo "FAIL $1"
}

# Schema inference reads only the intact Schema message, so it must still succeed. Asserting this
# pins the failure to the data-read path: a DESCRIBE-based test of the cases below would be vacuous.
$CLICKHOUSE_LOCAL --query "DESC file('${TMP_DIR}/bad_frame.arrows', ArrowStream)" > /dev/null 2>&1 \
    && echo 'OK describe' || echo 'FAIL describe'

check bad_frame.arrows INCORRECT_DATA
check prefix_mismatch.arrows INCORRECT_DATA
check aggregate_too_large.arrows INCORRECT_DATA
# Not corrupt: a size the query cannot afford is a resource condition, not a data error.
check consistent_large.arrows MEMORY_LIMIT_EXCEEDED

# Well-formed compressed files must be unaffected, from either writer and both codecs.
for f in wellformed_lz4 wellformed_zstd ch_lz4; do
    $CLICKHOUSE_LOCAL --query "
        SELECT count(), sum(i), uniqExact(s) FROM file('${TMP_DIR}/${f}.arrows', ArrowStream)"
done
