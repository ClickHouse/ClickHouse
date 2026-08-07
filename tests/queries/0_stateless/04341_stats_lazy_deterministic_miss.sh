#!/usr/bin/env bash
# Tags: no-object-storage
#
# `no-object-storage`: the test corrupts the statistics payload directly inside
# the part directory on the local disk. On the s3 / azure object-storage disks the
# files in that directory are `DiskObjectStorageMetadata` pointer files, not the
# statistics payload, so overwriting them does not corrupt the stored statistics.
#
# A column whose statistics are declared in the table metadata but whose statistics
# are unreadable (here: corrupted) must fail the operation that needs them, with the
# column and part in the error message, and the failure must stay retryable: it must
# not be recorded in the per-part estimates cache as "no statistics" (which would
# silently disable statistics for the column until the part object is rebuilt).
# Queries that do not need the corrupted statistics must be unaffected.
# Related: https://github.com/ClickHouse/ClickHouse/pull/104691

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_stats_miss SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_stats_miss (a UInt64, b UInt64 STATISTICS(basic))
ENGINE = MergeTree ORDER BY tuple()
-- min_bytes_for_wide_part = 0 forces a Wide (non-Compact) part so every file is a
-- separate on-disk entity; min_bytes_for_full_part_storage = 0 keeps the part in
-- full-part storage so that statistics.packed is an independent file rather
-- than being embedded inside data.packed. The Python corruption step below
-- relies on both properties.
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_stats_miss SELECT number, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_stats_miss'")
PART=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_stats_miss' AND active LIMIT 1")

# Per-column statistics are packed into a single `statistics.packed` file: a small
# index (`[version][count]` then `[name][offset][size]` per column) followed by the
# per-column compressed blobs. Zero out only column `b`'s blob, leaving the index
# and column `a`'s blob intact, so the container still parses but deserializing `b`
# deterministically fails. The total file size is preserved so the part's on-load
# size check (`Checksum::checkSize`, which does not verify content) still passes and
# the part is not marked broken.
PACKED_FILE="${DATA_PATH}${PART}/statistics.packed"
python3 - "$PACKED_FILE" <<'PY'
import struct, sys

path = sys.argv[1]
data = bytearray(open(path, "rb").read())
pos = 0
pos += 1  # version (UInt8)
num_files = struct.unpack_from("<Q", data, pos)[0]; pos += 8
for _ in range(num_files):
    shift = length = 0
    while True:  # readVarUInt name length
        byte = data[pos]; pos += 1
        length |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    name = bytes(data[pos:pos + length]); pos += length
    offset = struct.unpack_from("<Q", data, pos)[0]; pos += 8
    size = struct.unpack_from("<Q", data, pos)[0]; pos += 8
    if name == b"statistics_b.stats":
        for i in range(offset, offset + size):
            data[i] = 0
open(path, "wb").write(data)
PY

# Drop the per-part estimates cache populated during INSERT and rebuild the part
# object from disk so the SELECTs below load statistics for real.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_stats_miss"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_stats_miss"

prune_query="SELECT count() FROM t_stats_miss WHERE b > 500
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0,
             use_statistics_cache = 1,
             enable_analyzer = 1, enable_parallel_replicas = 0"

# Both queries prune on `b`, so each one asks the part for `b`'s statistics and
# must fail on the corrupted blob with the column and part in the message. The
# second query must fail the same way: the failure must not be negatively cached
# as "no statistics for b".
for i in 1 2; do
    echo "query $i:"
    ${CLICKHOUSE_CLIENT} -q "$prune_query FORMAT Null" 2>&1 \
        | grep -o "while loading statistics for column b from file statistics_b.stats in packed file statistics.packed of part ${PART}" \
        | head -n 1
done

# A query that does not need `b`'s statistics is unaffected.
echo "without statistics pruning:"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_stats_miss WHERE b > 500 SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_miss SYNC"
