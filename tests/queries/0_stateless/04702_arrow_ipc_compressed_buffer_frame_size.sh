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
# condition, which one case asserts. Neither is a ZSTD payload whose frame layout the per-buffer
# header cannot describe (several concatenated frames, or a leading skippable frame), nor an empty
# buffer whose frame honestly declares zero: those decompress correctly and must still be read.

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

$CLICKHOUSE_LOCAL --query "
    SELECT number AS i, toString(number) AS s, number * 0.5 AS f FROM numbers(4000)
    INTO OUTFILE '${TMP_DIR}/ch_zstd.arrows' TRUNCATE FORMAT ArrowStream
    SETTINGS output_format_arrow_compression_method = 'zstd'"

python3 - "$TMP_DIR" <<'PYEOF'
import struct, sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
LZ4_MAGIC = b"\x04\x22\x4d\x18"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

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
ch_zstd = bytearray(open(f"{out}/ch_zstd.arrows", "rb").read())


def prefix_offsets(data, magic=LZ4_MAGIC):
    """Offsets of the 8-byte uncompressed-length prefixes, found via the codec's frame magic."""
    res, i = [], 0
    while True:
        i = data.find(magic, i)
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
zstd_offs = prefix_offsets(ch_zstd, ZSTD_MAGIC)
assert len(arrow_offs) >= 2, f"expected >= 2 compressed buffers, got {len(arrow_offs)}"
assert zstd_offs, "expected a ZSTD-compressed buffer"


def compressed_buffer_spans(data):
    """Spans of every compressed buffer in the file's single RecordBatch message, from its metadata.

    A buffer's length is only in the RecordBatch flatbuffer, not in the payload, so read it there:
    Message(version, header_type, header, bodyLength) -> RecordBatch(length, nodes, buffers, ...),
    whose `buffers` is a vector of inline (offset, length) pairs. Both tables are read through their
    vtables, so a field the writer omitted is simply absent rather than misread.
    """
    def u16(o):
        return struct.unpack_from("<H", data, o)[0]

    def u32(o):
        return struct.unpack_from("<I", data, o)[0]

    def i64(o):
        return struct.unpack_from("<q", data, o)[0]

    def field(table, idx):
        """Absolute offset of field `idx` of the table at `table`, or None when absent."""
        vtable = table - struct.unpack_from("<i", data, table)[0]
        pos = 4 + 2 * idx
        if pos + 2 > u16(vtable):
            return None
        rel = u16(vtable + pos)
        return None if rel == 0 else table + rel

    spans, pos = [], 0
    while pos + 8 <= len(data):
        if u32(pos) != 0xFFFFFFFF:
            break
        meta_len, = struct.unpack_from("<i", data, pos + 4)
        if meta_len == 0:  # end-of-stream marker
            break
        meta = pos + 8
        msg = meta + u32(meta)
        body = (meta + meta_len + 7) & ~7
        body_len = i64(field(msg, 3)) if field(msg, 3) is not None else 0
        if data[field(msg, 1)] == 3:  # Message.header_type == RecordBatch
            header = field(msg, 2)
            batch = header + u32(header)
            buffers = field(batch, 2)
            vec = buffers + u32(buffers)
            for k in range(u32(vec)):
                offset, length = i64(vec + 4 + 16 * k), i64(vec + 12 + 16 * k)
                if length > 8:  # a compressed buffer: 8-byte prefix plus a payload
                    spans.append((body + offset + 8, body + offset + length))
        pos = body + body_len
    return spans


def repack_zstd(data, span, frames):
    """Replace a ZSTD payload with `frames(decompressed_bytes)`, padded back to its original length.

    Padding is a ZSTD skippable frame, which the decompressor ignores, so the payload keeps its byte
    length and every buffer offset in the RecordBatch stays valid without patching the metadata.
    """
    start, end = span
    declared, = struct.unpack_from("<q", data, start - 8)
    raw = pa.decompress(bytes(data[start:end]), decompressed_size=declared, codec="zstd", asbytes=True)
    new = frames(raw)
    # Skippable frame: magic(4) size(4) content(size), so any pad of 8 bytes or more fits.
    room = (end - start) - len(new)
    assert room >= 8, f"replacement payload is {8 - room} bytes too long to pad"
    new += struct.pack("<II", 0x184D2A50, room - 8) + b"\x00" * (room - 8)
    out = bytearray(data)
    out[start:end] = new
    return out


def zstd_frame(raw):
    """One ZSTD frame that pledges its content size, as ClickHouse's own writer emits."""
    return pa.Codec("zstd", compression_level=19).compress(raw, asbytes=True)


def batch_meta(data):
    """The RecordBatch message's body offset, body length and per-buffer (offset, length) field offsets."""
    def u16(o):
        return struct.unpack_from("<H", data, o)[0]

    def u32(o):
        return struct.unpack_from("<I", data, o)[0]

    def i64(o):
        return struct.unpack_from("<q", data, o)[0]

    def field(table, idx):
        vtable = table - struct.unpack_from("<i", data, table)[0]
        pos = 4 + 2 * idx
        if pos + 2 > u16(vtable):
            return None
        rel = u16(vtable + pos)
        return None if rel == 0 else table + rel

    pos = 0
    while pos + 8 <= len(data):
        if u32(pos) != 0xFFFFFFFF:
            break
        meta_len, = struct.unpack_from("<i", data, pos + 4)
        if meta_len == 0:
            break
        meta = pos + 8
        msg = meta + u32(meta)
        body = (meta + meta_len + 7) & ~7
        body_len_field = field(msg, 3)
        body_len = i64(body_len_field) if body_len_field is not None else 0
        if data[field(msg, 1)] == 3:  # Message.header_type == RecordBatch
            header = field(msg, 2)
            batch = header + u32(header)
            buffers = field(batch, 2)
            vec = buffers + u32(buffers)
            entries = [(vec + 4 + 16 * k, vec + 12 + 16 * k) for k in range(u32(vec))]
            return body, body_len, body_len_field, entries
        pos = body + body_len
    raise AssertionError("no RecordBatch message")


def lone_empty_zstd_frame_buffer(data, prefix_value):
    """Repoint a naturally-empty buffer at a new one whose payload is a LONE empty ZSTD frame.

    The payload must be exactly one frame, otherwise the size gate sends it down the `nullopt` path
    and the arm is vacuous - so it cannot be padded with a skippable frame the way `repack_zstd`
    does. Instead the buffer is appended past the body and an existing zero-length buffer (a
    non-nullable column's absent validity bitmap) is pointed at it, which leaves every other
    buffer's offset untouched.
    """
    d = bytearray(data)
    body, body_len, body_len_field, entries = batch_meta(d)
    frame = pa.Codec("zstd", compression_level=1).compress(b"", asbytes=True)
    # Not the skippable shape: an empty *real* frame keeps ZSTD's own magic.
    assert frame[:4] == ZSTD_MAGIC, frame[:4].hex()
    payload = struct.pack("<q", prefix_value) + frame
    target = next(((o, l) for o, l in entries if struct.unpack_from("<q", d, l)[0] == 0), None)
    assert target, "no zero-length buffer to repoint"
    d[body + body_len:body + body_len] = payload + b"\x00" * (-len(payload) % 8)
    struct.pack_into("<q", d, body_len_field, body_len + len(payload) + (-len(payload) % 8))
    struct.pack_into("<q", d, target[0], body_len)
    struct.pack_into("<q", d, target[1], len(payload))
    return bytes(d)

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

# The cases below cover the ZSTD branch of the frame lookup, which the LZ4-derived cases above never
# reach: the two codecs read their frame headers through different APIs.

# Case 5: a ZSTD prefix disagreeing with the size its own frame declares (ClickHouse's writer
# records one). Far larger than the real size but below the allocator's ceiling, as in case 2.
d = bytearray(ch_zstd)
struct.pack_into("<q", d, zstd_offs[0], 100 * 1024 ** 3)
open(f"{out}/zstd_prefix_mismatch.arrows", "wb").write(bytes(d))

# Case 6: not a parsable ZSTD frame. Clear the magic of the first buffer's payload.
d = bytearray(ch_zstd)
d[zstd_offs[0] + 8:zstd_offs[0] + 12] = b"\x00\x00\x00\x00"
open(f"{out}/zstd_bad_frame.arrows", "wb").write(bytes(d))

# Cases 7 and 8 (NOT corrupt): payload shapes whose first frame header does not describe the whole
# payload, so its declared size is not comparable to the prefix. ZSTD decompression accepts both -
# it sums every concatenated frame and skips skippable ones - so both must still be read.
zstd_spans = compressed_buffer_spans(ch_zstd)
assert zstd_spans, "expected a compressed buffer in the RecordBatch metadata"

d = repack_zstd(ch_zstd, zstd_spans[0],
                lambda raw: zstd_frame(raw[:len(raw) // 2]) + zstd_frame(raw[len(raw) // 2:]))
open(f"{out}/zstd_multi_frame.arrows", "wb").write(bytes(d))

d = repack_zstd(ch_zstd, zstd_spans[0],
                lambda raw: struct.pack("<II", 0x184D2A50, 4) + b"\x00" * 4 + zstd_frame(raw))
open(f"{out}/zstd_skippable_prefix.arrows", "wb").write(bytes(d))

# Case 9: a lone empty ZSTD frame truthfully declares 0, so a positive prefix disagrees with it and
# must be rejected before it is allocated for. Zero is a content size here, not a "declares nothing"
# marker - ZSTD signals that separately, with ZSTD_CONTENTSIZE_UNKNOWN.
open(f"{out}/zstd_empty_frame_forged_prefix.arrows", "wb").write(
    lone_empty_zstd_frame_buffer(ch_zstd, 100 * 1024 ** 3))

# Case 10 (NOT corrupt): the same lone empty frame with the prefix it really describes. The
# comparison must accept the 0 it now compares rather than over-rejecting an empty buffer.
open(f"{out}/zstd_empty_frame_honest_prefix.arrows", "wb").write(
    lone_empty_zstd_frame_buffer(ch_zstd, 0))
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
check zstd_prefix_mismatch.arrows INCORRECT_DATA
check zstd_bad_frame.arrows INCORRECT_DATA
# The rejection must come from the frame comparison, so match its message: a bare INCORRECT_DATA
# would also pass if the allocator guard caught it instead, leaving the comparison untested.
check zstd_empty_frame_forged_prefix.arrows 'codec frame declares 0'
# Not corrupt: a size the query cannot afford is a resource condition, not a data error.
check consistent_large.arrows MEMORY_LIMIT_EXCEEDED

# Well-formed compressed files must be unaffected, from either writer and both codecs, including the
# two ZSTD payload shapes whose leading frame header describes less than the whole payload, and an
# empty buffer whose frame honestly declares the 0 the comparison now sees.
for f in wellformed_lz4 wellformed_zstd ch_lz4 ch_zstd zstd_multi_frame zstd_skippable_prefix \
         zstd_empty_frame_honest_prefix; do
    $CLICKHOUSE_LOCAL --query "
        SELECT count(), sum(i), uniqExact(s) FROM file('${TMP_DIR}/${f}.arrows', ArrowStream)"
done
