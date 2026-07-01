#!/usr/bin/env bash
# Tags: no-object-storage
#
# `no-object-storage`: the test corrupts the statistics payload directly inside
# the part directory on the local disk. On the s3 / azure object-storage disks the
# files in that directory are `DiskObjectStorageMetadata` pointer files, not the
# statistics payload, so overwriting them does not corrupt the stored statistics.
#
# Regression test for the deterministic-miss negative cache in
# `IMergeTreeDataPart::getEstimates`. A column whose statistics are declared in
# the table metadata but whose statistics are unreadable (here: corrupted) must be
# probed at most once per in-memory part object. Before the fix, only columns with
# *no* declared statistics were recorded in `estimates_attempted_columns`, so a
# metadata-declared column with broken statistics was re-read (and re-warned) on
# every query.
# Related: https://github.com/ClickHouse/ClickHouse/pull/104691

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_stats_miss SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_stats_miss (a UInt64, b UInt64 STATISTICS(minmax))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0"

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

QID1="${CLICKHOUSE_DATABASE}_stats_miss_1"
QID2="${CLICKHOUSE_DATABASE}_stats_miss_2"

# Both queries prune on `b`, so each one asks the part for `b`'s statistics. The
# corrupted-statistics warning is asserted below via `system.text_log`; suppress its
# streaming to the client with `--send_logs_level=error` so it does not reach stderr.
for QID in "$QID1" "$QID2"; do
    ${CLICKHOUSE_CLIENT} --send_logs_level=error --query_id="$QID" -q "
    SELECT count() FROM t_stats_miss WHERE b > 500
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0,
             use_statistics_cache = 0, enable_analyzer = 1, enable_parallel_replicas = 0
    FORMAT Null"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"

# The first query probes the corrupted blob once and logs a warning; the second
# query must find `b` negatively cached and emit no further warning.
echo "first query warnings:"
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND query_id = '${QID1}'
  AND message LIKE '%Cannot load statistics for column b%'"
echo "second query warnings:"
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND query_id = '${QID2}'
  AND message LIKE '%Cannot load statistics for column b%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_miss SYNC"
