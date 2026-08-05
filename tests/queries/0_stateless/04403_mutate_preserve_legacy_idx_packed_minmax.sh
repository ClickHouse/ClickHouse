#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `no-fasttest`: rewriting the `skp_idx.packed` footer on local disk is not reliably
# available on the Fast test macOS runner.
# `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: the fixture edits real
# local part files and relies on ATTACH recomputing `checksums.txt` from them.
# `no-random-merge-tree-settings`: the fixture targets a standalone index file at a fixed granule
# count; the settings it needs are pinned in the CREATE below.
#
# Packed-archive counterpart of 04402: rebuilding `skp_idx.packed` for a recomputed index (mm_w)
# must preload the surviving members of the preserved index (mm_v), including a legacy `.idx`
# data member. It used to preload only the mark, dropping mm_v's data. Issue #109595.
#
# Legacy shape fabricated by rewriting the packed footer to rename mm_v's `.idx2` member to
# `.idx` (byte-identical payloads for a non-nullable column), then ATTACH and `ALTER UPDATE` w.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_legacy_packed SYNC"

# v = k and w = k are monotone, so each minmax index prunes a point query to a
# single granule. `index_granularity` = 100 over 2000 rows gives 20 granules.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_legacy_packed
(
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX mm_v v TYPE minmax GRANULARITY 1,
    INDEX mm_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 1000000,
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_legacy_packed (k, v, w) SELECT number, number, number FROM numbers(2000)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_legacy_packed FINAL"

# Detach so we can rewrite the packed archive, then downgrade mm_v to the legacy
# ".idx" layout inside skp_idx.packed.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_packed DETACH PARTITION tuple() SETTINGS mutations_sync = 2"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_legacy_packed'")
PART_DIR=$(find "${DATA_PATH}detached" -maxdepth 1 -type d -name 'all_*' | head -1)

chmod u+w "${PART_DIR}/skp_idx.packed"
# Rewrite the packed footer, renaming skp_idx_mm_v.idx2 -> skp_idx_mm_v.idx.
# The footer is: version(1B) num_files(u64) then per file [varint name_len, name,
# u64 offset, u64 size, and in v1+ u64 uncompressed_size], followed by the
# concatenated data region. Renaming one member shrinks its name by 1 byte, so
# every offset must be recomputed. The extra per-entry fields (v1 adds
# uncompressed_size) are carried through unchanged.
python3 - "${PART_DIR}/skp_idx.packed" 'skp_idx_mm_v.idx2' 'skp_idx_mm_v.idx' <<'PY'
import sys, struct
path, rename_from, rename_to = sys.argv[1], sys.argv[2], sys.argv[3]
d = open(path, 'rb').read()
def rvar(b, o):
    shift = res = 0
    while True:
        x = b[o]; o += 1; res |= (x & 0x7f) << shift
        if not (x & 0x80): break
        shift += 7
    return res, o
def wvar(n):
    out = bytearray()
    while True:
        x = n & 0x7f; n >>= 7
        out.append(x | 0x80 if n else x)
        if not n: break
    return bytes(out)
off = 0
ver = d[off]; off += 1
# v0: [offset, size]; v1+: [offset, size, uncompressed_size].
num_u64 = 3 if ver >= 1 else 2
(num,) = struct.unpack_from('<Q', d, off); off += 8
entries = []
for _ in range(num):
    ln, off = rvar(d, off); name = d[off:off+ln].decode(); off += ln
    (o2,) = struct.unpack_from('<Q', d, off); off += 8
    (sz,) = struct.unpack_from('<Q', d, off); off += 8
    extra = []
    for _ in range(num_u64 - 2):
        (e,) = struct.unpack_from('<Q', d, off); off += 8
        extra.append(e)
    entries.append((name, o2, sz, extra))
members = {name: d[o:o+sz] for name, o, sz, _ in entries}
# Fail loudly rather than silently rewriting an equivalent current-format archive: if the member
# is not found, the fixture would produce no legacy member at all and the test would pass for both
# the fixed and the unfixed implementation.
matches = sum(1 for n, _, _, _ in entries if n == rename_from)
if matches != 1:
    raise SystemExit(f"expected exactly one {rename_from} member in the archive, found {matches}")
renamed = [(rename_to if n == rename_from else n, sz, extra) for n, _, sz, extra in entries]
footer = 1 + 8 + sum(len(wvar(len(n))) + len(n) + 8 * num_u64 for n, _, _ in renamed)
out = bytearray(); out.append(ver); out += struct.pack('<Q', len(renamed))
cur = footer
for n, sz, extra in renamed:
    out += wvar(len(n)); out += n.encode()
    out += struct.pack('<Q', cur); out += struct.pack('<Q', sz)
    for e in extra:
        out += struct.pack('<Q', e)
    cur += sz
for (orig, _, _, _), (n, _, _) in zip(entries, renamed):
    out += members[orig]
open(path, 'wb').write(out)
PY
rm -f "${PART_DIR}/checksums.txt"

${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_packed ATTACH PARTITION tuple()"

# Sanity: the legacy ".idx" minmax member inside the archive is recognized and
# prunes to one granule.
echo "before:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_legacy_packed WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"

# Rebuild the archive: `ALTER UPDATE` touches only w, so mm_w is recomputed (and the
# packed archive is rewritten) while mm_v is preserved. Before the fix the legacy
# ".idx" data member of mm_v was dropped from the new archive and no longer pruned.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_legacy_packed UPDATE w = w + 0 WHERE 1 SETTINGS mutations_sync = 2"

${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_legacy_packed SETTINGS check_query_single_value_result = 1"

# The preserved legacy mm_v index must still prune to one granule (was 20/20 before
# the fix), and the recomputed mm_w must prune too.
echo "after:"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_legacy_packed WHERE v = 42) WHERE explain ILIKE '%Granules: 1/20%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_legacy_packed WHERE w = 42) WHERE explain ILIKE '%Granules: 1/20%'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_legacy_packed WHERE v = 42"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_legacy_packed SYNC"
