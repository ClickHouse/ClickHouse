#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage, no-random-merge-tree-settings
#
# `system.parts.secondary_indices_materialized` must require the marks file of
# every index substream, not just the data file: `MergeTreeIndexReader` loads a
# marks file for each stream it opens, so a part whose `checksums.txt` lists
# `skp_idx_<name>.idx2` but not the matching marks file has an unusable index
# and reporting it as materialized would be a false positive.
#
# The shape is fabricated by rewriting the part's `checksums.txt` without the
# marks entry (re-emitted in the plain-text format version 2, which the server
# still reads) and reloading the part with `DETACH TABLE` / `ATTACH TABLE`.
#
# no-fasttest: local-disk part-file surgery (as in 04870).
# no-object-storage/-shared/-replicated: relies on the local on-disk file layout.
# no-random-merge-tree-settings: the surgery assumes standalone (non-packed)
# index files, so `packed_skip_index_max_bytes` is pinned to 0 below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_missing_marks SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_missing_marks
(
    k UInt64,
    v UInt64,
    INDEX mm_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         index_granularity = 100, replace_long_file_name_to_hash = 0,
         packed_skip_index_max_bytes = 0,
         columns_and_secondary_indices_sizes_lazy_calculation = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_missing_marks (k, v) SELECT number, number FROM numbers(2000)"

materialized () {
    ${CLICKHOUSE_CLIENT} -q "
    SELECT secondary_indices_materialized
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_missing_marks' AND active AND rows > 0
    ORDER BY name"
}

echo "after insert:"
materialized

part_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_missing_marks' AND active AND rows > 0 LIMIT 1")
marks_file=$(basename "$(ls "${part_path}"skp_idx_mm_v.*mrk*)")

# No background rewrite of the part while its files are edited underneath it.
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_missing_marks"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_missing_marks"

# Rewrite checksums.txt without the marks entry: decompress the version-4 body,
# parse the version-3 binary it holds, re-emit as plain-text version 2 keeping
# the true sizes and hashes of every other file, then drop the marks file itself.
tail -c +29 "${part_path}checksums.txt" > "${part_path}checksums.v4body"
${CLICKHOUSE_COMPRESSOR} --decompress < "${part_path}checksums.v4body" > "${part_path}checksums.v3"
python3 - "${part_path}checksums.v3" "${part_path}checksums.txt" "$marks_file" <<'EOF'
import sys

data = open(sys.argv[1], 'rb').read()
pos = 0

def read_varuint():
    global pos
    result, shift = 0, 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result
        shift += 7

def read_u64():
    global pos
    v = int.from_bytes(data[pos:pos + 8], 'little'); pos += 8
    return v

entries = []
for _ in range(read_varuint()):
    name_len = read_varuint()
    name = data[pos:pos + name_len].decode(); pos += name_len
    file_size = read_varuint()
    hash_low, hash_high = read_u64(), read_u64()
    is_compressed = data[pos]; pos += 1
    unc = None
    if is_compressed:
        unc = (read_varuint(), read_u64(), read_u64())
    entries.append((name, file_size, hash_low, hash_high, is_compressed, unc))

entries = [e for e in entries if e[0] != sys.argv[3]]
with open(sys.argv[2], 'w') as out:
    out.write('checksums format version: 2\n')
    out.write(f'{len(entries)} files:\n')
    for name, file_size, hash_low, hash_high, is_compressed, unc in entries:
        out.write(f'{name}\n\tsize: {file_size}\n\thash: {hash_low} {hash_high}\n\tcompressed: {1 if is_compressed else 0}\n')
        if is_compressed:
            out.write(f'\tuncompressed size: {unc[0]}\n\tuncompressed hash: {unc[1]} {unc[2]}\n')
EOF
rm "${part_path}checksums.v4body" "${part_path}checksums.v3" "${part_path}${marks_file}"

${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_missing_marks"

# The index data file is still owned (listed in checksums.txt and on disk), but
# its marks file is neither: the reader cannot use this index, so it must not be
# reported as materialized.
echo "data file still on disk:"
if ls "${part_path}"skp_idx_mm_v.idx* >/dev/null 2>&1; then echo 1; else echo 0; fi
echo "with missing marks file:"
materialized

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_missing_marks SYNC"
