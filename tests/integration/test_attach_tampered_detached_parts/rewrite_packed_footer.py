#!/usr/bin/env python3
# Rewrites the `skp_idx.packed` footer of a detached part, renaming one member
# (used to downgrade `skp_idx_mm_v.idx2` to the legacy `skp_idx_mm_v.idx`).
# Shipped into the server container and executed there by
# `test_mutate_preserve_legacy_idx_packed_minmax`. Taken verbatim from the
# stateless test 04403_mutate_preserve_legacy_idx_packed_minmax.sh.
#
# The footer is: version(1B) num_files(u64) then per file [varint name_len, name,
# u64 offset, u64 size, and in v1+ u64 uncompressed_size], followed by the
# concatenated data region. Renaming one member shrinks its name by 1 byte, so
# every offset must be recomputed. The extra per-entry fields (v1 adds
# uncompressed_size) are carried through unchanged.
import struct
import sys

path, rename_from, rename_to = sys.argv[1], sys.argv[2], sys.argv[3]
d = open(path, "rb").read()


def rvar(b, o):
    shift = res = 0
    while True:
        x = b[o]
        o += 1
        res |= (x & 0x7F) << shift
        if not (x & 0x80):
            break
        shift += 7
    return res, o


def wvar(n):
    out = bytearray()
    while True:
        x = n & 0x7F
        n >>= 7
        out.append(x | 0x80 if n else x)
        if not n:
            break
    return bytes(out)


off = 0
ver = d[off]
off += 1
# v0: [offset, size]; v1+: [offset, size, uncompressed_size].
num_u64 = 3 if ver >= 1 else 2
(num,) = struct.unpack_from("<Q", d, off)
off += 8
entries = []
for _ in range(num):
    ln, off = rvar(d, off)
    name = d[off : off + ln].decode()
    off += ln
    (o2,) = struct.unpack_from("<Q", d, off)
    off += 8
    (sz,) = struct.unpack_from("<Q", d, off)
    off += 8
    extra = []
    for _ in range(num_u64 - 2):
        (e,) = struct.unpack_from("<Q", d, off)
        off += 8
        extra.append(e)
    entries.append((name, o2, sz, extra))
members = {name: d[o : o + sz] for name, o, sz, _ in entries}
# Fail loudly rather than silently rewriting an equivalent current-format archive: if the member
# is not found, the fixture would produce no legacy member at all and the test would pass for both
# the fixed and the unfixed implementation.
matches = sum(1 for n, _, _, _ in entries if n == rename_from)
if matches != 1:
    raise SystemExit(f"expected exactly one {rename_from} member in the archive, found {matches}")
renamed = [(rename_to if n == rename_from else n, sz, extra) for n, _, sz, extra in entries]
footer = 1 + 8 + sum(len(wvar(len(n))) + len(n) + 8 * num_u64 for n, _, _ in renamed)
out = bytearray()
out.append(ver)
out += struct.pack("<Q", len(renamed))
cur = footer
for n, sz, extra in renamed:
    out += wvar(len(n))
    out += n.encode()
    out += struct.pack("<Q", cur)
    out += struct.pack("<Q", sz)
    for e in extra:
        out += struct.pack("<Q", e)
    cur += sz
for (orig, _, _, _), (n, _, _) in zip(entries, renamed):
    out += members[orig]
open(path, "wb").write(out)
