#!/usr/bin/env bash
# Tags: no-fasttest
# The corruption is in the RecordBatch body, not the Schema message, so schema inference must still
# succeed and only a data read reaches it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# Two writers, because the frame's content size is optional: ClickHouse records it, pyarrow omits it,
# and cases below need both.
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
SKIPPABLE = 0x184D2A50

# A String column contributes two buffers, so the batch carries more than one compressed buffer.
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
    assert len(data) < 16, "the 16-byte-lane path of xxh32 is not implemented"

    def rol(x, r):
        return ((x << r) | (x >> (32 - r))) & M

    h, idx = (seed + P5 + len(data)) & M, 0
    while idx + 4 <= len(data):
        lane, = struct.unpack_from("<I", data, idx)
        h = (rol((h + lane * P3) & M, 17) * P4) & M
        idx += 4
    while idx < len(data):
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


def i64(data, o):
    return struct.unpack_from("<q", data, o)[0]


def batch_meta(data):
    """The RecordBatch body offset and length, the bodyLength field offset, and the field offsets of
    every per-buffer (offset, length) pair - all of which live only in the flatbuffer, not the
    payload. Read through vtables, so a field the writer omitted is absent rather than misread.
    """
    def u16(o):
        return struct.unpack_from("<H", data, o)[0]

    def u32(o):
        return struct.unpack_from("<I", data, o)[0]

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
        if meta_len == 0:  # end-of-stream marker
            break
        meta = pos + 8
        msg = meta + u32(meta)
        body = (meta + meta_len + 7) & ~7
        body_len_field = field(msg, 3)
        body_len = i64(data, body_len_field) if body_len_field is not None else 0
        if data[field(msg, 1)] == 3:  # Message.header_type == RecordBatch
            header = field(msg, 2)
            batch = header + u32(header)
            buffers = field(batch, 2)
            vec = buffers + u32(buffers)
            entries = [(vec + 4 + 16 * k, vec + 12 + 16 * k) for k in range(u32(vec))]
            return body, body_len, body_len_field, entries
        pos = body + body_len
    raise AssertionError("no RecordBatch message")


def compressed_spans(data):
    """Absolute (start, end) of every compressed buffer's payload, past its 8-byte prefix."""
    body, _, _, entries = batch_meta(data)
    spans = []
    for off_f, len_f in entries:
        offset, length = i64(data, off_f), i64(data, len_f)
        if length > 8:  # a compressed buffer: 8-byte prefix plus a payload
            spans.append((body + offset + 8, body + offset + length))
    return spans


def repack_zstd(data, span, frames):
    """Replace a ZSTD payload with `frames(decompressed_bytes)`, padded back to its original length.

    The pad is a ZSTD skippable frame, which the decompressor ignores, so every buffer offset in the
    RecordBatch stays valid without patching the metadata.
    """
    start, end = span
    raw = pa.decompress(bytes(data[start:end]), decompressed_size=i64(data, start - 8),
                        codec="zstd", asbytes=True)
    new = frames(raw)
    room = (end - start) - len(new)
    assert room >= 8, f"replacement payload is {8 - room} bytes too long to pad"
    new += struct.pack("<II", SKIPPABLE, room - 8) + b"\x00" * (room - 8)
    d = bytearray(data)
    d[start:end] = new
    return d


def lone_frame_buffer(data, prefix_value, frame):
    """Repoint a naturally-empty buffer at a new one carrying exactly `frame`, appended past the body.

    No skippable pad, unlike `repack_zstd`: a second frame would take a different path and make the
    arm vacuous. Appending leaves every existing buffer's offset untouched.
    """
    d = bytearray(data)
    body, body_len, body_len_field, entries = batch_meta(d)
    payload = struct.pack("<q", prefix_value) + frame
    target = next(((o, l) for o, l in entries if i64(d, l) == 0), None)
    assert target, "no zero-length buffer to repoint"
    d[body + body_len:body + body_len] = payload + b"\x00" * (-len(payload) % 8)
    struct.pack_into("<q", d, body_len_field, body_len + len(payload) + (-len(payload) % 8))
    struct.pack_into("<q", d, target[0], body_len)
    struct.pack_into("<q", d, target[1], len(payload))
    return bytes(d)


def zstd_frame(raw):
    """One ZSTD frame that pledges its content size, as ClickHouse's own writer emits."""
    return pa.Codec("zstd", compression_level=19).compress(raw, asbytes=True)


def zstd_frame_without_declared_size(raw):
    """One ZSTD frame that omits Frame_Content_Size, which neither ClickHouse nor pyarrow emits.

    Descriptor 0 selects no content size, checksum or dictionary; the Window_Descriptor exponent sits
    in its top 5 bits. Raw blocks: 3-byte header, last-block in bit 0, type in bits 1-2, size above.
    """
    max_block = 1 << 17
    out = bytearray(ZSTD_MAGIC + bytes([0x00, (20 - 10) << 3]))
    pos = 0
    while True:
        block = raw[pos:pos + max_block]
        pos += len(block)
        last = 1 if pos >= len(raw) else 0
        out += struct.pack("<I", last | (len(block) << 3))[:3] + block
        if last:
            return bytes(out)


def empty_zstd_frame():
    """A real empty frame, which truthfully declares 0 and keeps ZSTD's own (not skippable) magic."""
    frame = pa.Codec("zstd", compression_level=1).compress(b"", asbytes=True)
    assert frame[:4] == ZSTD_MAGIC, frame[:4].hex()
    return frame


def empty_lz4_frame(declare_zero):
    """An LZ4 frame carrying no blocks: magic, FLG, BD, [contentSize], HC, EndMark.

    With `declare_zero` the optional content size is present and reads 0, the same value the API
    reports when it is absent - the two states must not be conflated.
    """
    flg = 0x40 | (0x08 if declare_zero else 0)
    header = bytes([flg, 0x40]) + (struct.pack("<Q", 0) if declare_zero else b"")
    return LZ4_MAGIC + header + bytes([(xxh32(header) >> 8) & 0xFF]) + struct.pack("<I", 0)


def lz4_frame_with_stored_block(content_checksum=b"", trailing=b""):
    """An LZ4 frame with one stored block, optionally followed by extra bytes.

    A stored block expands to exactly its own bytes, so the bound stays 8 whatever is appended, which
    keeps these cases about the suffix. All-ones because the target buffer is a validity bitmap.
    """
    flg = 0x40 | (0x04 if content_checksum else 0)
    header = bytes([flg, 0x70])  # blockSizeID 7 (4 MB), block independence off
    block = struct.pack("<I", 0x80000000 | 8) + b"\xff" * 8
    return (LZ4_MAGIC + header + bytes([(xxh32(header) >> 8) & 0xFF]) + block
            + struct.pack("<I", 0) + content_checksum + trailing)


def lz4_frame_with_tiny_compressed_blocks(nblocks, literals=4):
    """An LZ4 frame of `nblocks` literals-only compressed blocks, declaring no content size.

    Each block stores 1 + `literals` bytes and produces `literals`, so crediting a whole
    maxBlockSize per block would bound the frame thousands of times above what it can emit.
    """
    header = bytes([0x40, 0x70])  # no content size, blockSizeID 7 (4 MB)
    block = bytes([literals << 4]) + b"\xff" * literals
    return (LZ4_MAGIC + header + bytes([(xxh32(header) >> 8) & 0xFF])
            + (struct.pack("<I", len(block)) + block) * nblocks + struct.pack("<I", 0))


def write(name, data):
    open(f"{out}/{name}.arrows", "wb").write(bytes(data))


def lz4_block_bound(data, prefix_off):
    """What the frame at `prefix_off` can produce: per block, the least of the frame's maximum block
    size and what the block's own stored bytes can encode."""
    flg = data[prefix_off + 12]
    p = prefix_off + 8 + 7 + (8 if flg & 0x08 else 0)
    max_block = 64 * 1024 << (2 * (((data[prefix_off + 13] >> 4) & 7) - 4))
    bound = 0
    while True:
        bh, = struct.unpack_from("<I", data, p)
        p += 4
        if bh == 0:
            return bound
        n = bh & 0x7FFFFFFF
        bound += n if bh & 0x80000000 else min(max_block, n * 255)
        p += n + (4 if flg & 0x10 else 0)


# Larger than the file but below what one block can produce, so `consistent_large` is a resource
# condition rather than a data error. Asserted, because the block bound follows the writer's output.
big = 2 * 1024 ** 2
# Far larger than any real size, but below the allocator's ceiling, so the frame comparison rather
# than the total is what rejects these.
forged = 100 * 1024 ** 3

# Not a parsable frame: clear the first buffer payload's magic. Both codecs, which read their frame
# headers through different APIs.
d = bytearray(arrow_lz4)
d[arrow_offs[0] + 8:arrow_offs[0] + 12] = b"\x00\x00\x00\x00"
write("bad_frame", d)

d = bytearray(ch_zstd)
d[zstd_offs[0] + 8:zstd_offs[0] + 12] = b"\x00\x00\x00\x00"
write("zstd_bad_frame", d)

# A prefix disagreeing with the size its own frame pledges, for both codecs.
d = bytearray(ch_lz4)
struct.pack_into("<q", d, ch_offs[0], forged)
write("prefix_mismatch", d)

d = bytearray(ch_zstd)
struct.pack_into("<q", d, zstd_offs[0], forged)
write("zstd_prefix_mismatch", d)

# Two forged prefixes at once in frames that pledge nothing, so only their blocks bound them. Every
# buffer is checked, so the first already rejects the file rather than any accumulated total.
d = bytearray(arrow_lz4)
half = (1 << 61) + 8
struct.pack_into("<q", d, arrow_offs[0], half)
struct.pack_into("<q", d, arrow_offs[1], half)
write("no_declared_size_forged_prefixes", d)

# NOT corrupt: prefix and frame agree on a size that one block really can produce. Larger than the
# file, so reading it is a resource condition rather than a data error.
d = bytearray(ch_lz4)
assert big <= lz4_block_bound(d, ch_offs[0]), "big exceeds what the frame's blocks can produce"
struct.pack_into("<q", d, ch_offs[0], big)
set_frame_content_size(d, ch_offs[0], big)
write("consistent_large", d)

# One byte past that: a pledge is enforced exactly, so this is rejected for disagreeing with the
# pledge and not for exceeding the block bound. The two reasons must not be conflated.
d = bytearray(ch_lz4)
struct.pack_into("<q", d, ch_offs[0], big + 1)
set_frame_content_size(d, ch_offs[0], big)
write("pledge_mismatch", d)

# A frame pledging more than its blocks can produce describes no possible frame, so it is rejected on
# its own terms. The prefix agrees with neither, so comparing only prefix against bound would allocate.
d = bytearray(ch_lz4)
set_frame_content_size(d, ch_offs[0], 1024 ** 3)
struct.pack_into("<q", d, ch_offs[0], big)
write("lz4_pledge_above_blocks", d)

# A recorded size of 0 over blocks that do produce data: the one shape where "recorded 0" and "not
# recorded" genuinely differ. Reading it as an exact size would reject the honest prefix.
d = bytearray(ch_lz4)
set_frame_content_size(d, ch_offs[0], 0)
write("lz4_zero_size_over_blocks", d)

# A pyarrow LZ4 frame omits the content size, so nothing pledges a size to contradict the prefix.
d = bytearray(arrow_lz4)
struct.pack_into("<q", d, arrow_offs[0], forged)
write("lz4_no_declared_size_forged_prefix", d)

# NOT corrupt: payload shapes whose leading frame header describes less than the whole payload.
# ZSTD sums concatenated frames and skips skippable ones, so both must still be read. With a forged
# prefix the disagreement must be rejected, which reading only the first frame's header would miss.
zstd_span = compressed_spans(ch_zstd)[0]
for name, frames in (
    ("zstd_multi_frame",
     lambda raw: zstd_frame(raw[:len(raw) // 2]) + zstd_frame(raw[len(raw) // 2:])),
    ("zstd_skippable_prefix",
     lambda raw: struct.pack("<II", SKIPPABLE, 4) + b"\x00" * 4 + zstd_frame(raw)),
):
    write(name, repack_zstd(ch_zstd, zstd_span, frames))
    d = repack_zstd(ch_zstd, zstd_span, frames)
    struct.pack_into("<q", d, zstd_span[0] - 8, forged)
    write(f"{name}_forged_prefix", d)

# An empty frame produces nothing, so any positive prefix over it is forged, and the honest 0 must
# still be accepted. Zero is a real content size here, not a "declares nothing" marker: ZSTD signals
# that separately with ZSTD_CONTENTSIZE_UNKNOWN, and LZ4 cannot signal it at all.
write("zstd_empty_frame_forged_prefix", lone_frame_buffer(ch_zstd, forged, empty_zstd_frame()))
write("zstd_empty_frame_honest_prefix", lone_frame_buffer(ch_zstd, 0, empty_zstd_frame()))
write("lz4_empty_frame_forged_prefix", lone_frame_buffer(ch_lz4, forged, empty_lz4_frame(False)))
write("lz4_empty_frame_zero_size_forged_prefix",
      lone_frame_buffer(ch_lz4, forged, empty_lz4_frame(True)))

# Bytes after the frame's blocks. The walk stops at the end marker, so without checking what follows
# it a payload the decompressor will reject still gets a bound and is allocated for. Both prefixes are
# the 8 bytes the block really produces, so only the suffix is wrong. A whole checksum is NOT corrupt.
write("lz4_trailing_data", lone_frame_buffer(ch_lz4, 8, lz4_frame_with_stored_block(trailing=b"\x00")))
write("lz4_truncated_content_checksum",
      lone_frame_buffer(ch_lz4, 8, lz4_frame_with_stored_block(content_checksum=b"\x00\x00")))
write("lz4_content_checksum",
      lone_frame_buffer(ch_lz4, 8,
                        lz4_frame_with_stored_block(content_checksum=struct.pack("<I", xxh32(b"\xff" * 8)))))

# Many tiny compressed blocks declaring no content size. What each block's own bytes can encode has
# to bound it too: 256 blocks of 5 stored bytes emit 1 KiB, so crediting a whole maxBlockSize each
# would accept a forged 512 MiB prefix and allocate for it before decompression rejected it. The
# honest-prefix file is NOT corrupt and must still be read.
tiny_blocks = lz4_frame_with_tiny_compressed_blocks(256)
write("lz4_tiny_blocks_forged_prefix", lone_frame_buffer(ch_lz4, 512 * 1024 ** 2, tiny_blocks))
write("lz4_tiny_blocks", lone_frame_buffer(ch_lz4, 256 * 4, tiny_blocks))

# A ZSTD frame that omits its content size, as a streaming writer emits: only its block structure
# bounds it. Appended as its own buffer because its Raw blocks do not fit the compressed payload's
# space. The honest-prefix file is NOT corrupt and must still be read.
body = b"row data to bound" * 64
frame = zstd_frame_without_declared_size(body)
write("zstd_no_declared_size_forged_prefix", lone_frame_buffer(ch_zstd, forged, frame))
write("zstd_no_declared_size", lone_frame_buffer(ch_zstd, len(body), frame))
# Truncated mid-block, so no block structure bounds it either. The prefix is honest, so only the
# frame is wrong: nothing can bound it and it must be rejected rather than bounded by the unknown.
write("zstd_no_declared_size_truncated", lone_frame_buffer(ch_zstd, len(body), frame[:-20]))
PYEOF

check() {
    $CLICKHOUSE_LOCAL --max_memory_usage="${3:-1G}" \
        --query "SELECT * FROM file('${TMP_DIR}/$1', ArrowStream) FORMAT Null" 2>&1 \
        | grep -F -q "$2" && echo "OK $1" || echo "FAIL $1"
}

# Schema inference reads only the intact Schema message, so it must still succeed. Asserting this
# pins the failures below to the data-read path.
$CLICKHOUSE_LOCAL --query "DESC file('${TMP_DIR}/bad_frame.arrows', ArrowStream)" > /dev/null 2>&1 \
    && echo 'OK describe' || echo 'FAIL describe'

check bad_frame.arrows INCORRECT_DATA
check zstd_bad_frame.arrows INCORRECT_DATA
# Match the frame comparison's own message: a bare INCORRECT_DATA would also pass if the allocator
# guard or the decompression call caught it instead, leaving the comparison untested.
check prefix_mismatch.arrows 'codec frame declares'
check no_declared_size_forged_prefixes.arrows 'codec frame declares'
check pledge_mismatch.arrows 'codec frame declares'
check zstd_prefix_mismatch.arrows 'codec frame declares'
check zstd_empty_frame_forged_prefix.arrows 'codec frame declares 0'
check zstd_multi_frame_forged_prefix.arrows 'codec frame declares'
check zstd_skippable_prefix_forged_prefix.arrows 'codec frame declares'
# An LZ4 frame with no blocks can produce nothing, so the comparison sees a 0 either way.
check lz4_empty_frame_forged_prefix.arrows 'codec frame declares 0'
check lz4_empty_frame_zero_size_forged_prefix.arrows 'codec frame declares 0'
check lz4_no_declared_size_forged_prefix.arrows 'codec frame declares'
check lz4_pledge_above_blocks.arrows 'blocks can produce at most'
# Match the walk's own messages, otherwise decompression rejecting these after allocating for them
# would pass too.
check lz4_trailing_data.arrows 'bytes after its LZ4 frame'
check lz4_truncated_content_checksum.arrows 'ends inside an LZ4 content checksum'
check lz4_tiny_blocks_forged_prefix.arrows 'codec frame declares'
check zstd_no_declared_size_forged_prefix.arrows 'codec frame declares'
check zstd_no_declared_size_truncated.arrows 'not a valid ZSTD frame'
# A size the query cannot afford is a resource condition. What a frame's blocks can produce is
# bounded, so the budget rather than the size is what has to be small.
check consistent_large.arrows MEMORY_LIMIT_EXCEEDED 1M

# Well-formed files must be unaffected: both writers, both codecs, and every shape above that is not
# corrupt.
for f in wellformed_lz4 wellformed_zstd ch_lz4 ch_zstd zstd_multi_frame zstd_skippable_prefix \
         zstd_empty_frame_honest_prefix lz4_zero_size_over_blocks zstd_no_declared_size \
         lz4_content_checksum lz4_tiny_blocks; do
    $CLICKHOUSE_LOCAL --query "
        SELECT count(), sum(i), uniqExact(s) FROM file('${TMP_DIR}/${f}.arrows', ArrowStream)"
done
