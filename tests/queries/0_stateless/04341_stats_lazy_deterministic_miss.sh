#!/usr/bin/env bash
# Tags: no-object-storage
#
# `no-object-storage`: the test corrupts the statistics payload directly inside
# the part directory on the local disk. On object-storage disks the files in that
# directory are pointer files, so overwriting them would not corrupt the statistics.
#
# Regression test for the deterministic-miss negative cache in
# `IMergeTreeDataPart::getEstimates`: a metadata-declared column whose statistics
# are unreadable (here: corrupted) must be probed at most once per part object.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Suppress the corrupted-statistics error streaming to the client; it is asserted
# below via `system.text_log` instead.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
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

# Zero out only column `b`'s blob in `statistics.packed`, leaving the index and
# column `a`'s blob intact, so deserializing `b` deterministically fails.
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

# Drop the per-part estimates cache populated during INSERT so the SELECTs below
# load statistics for real.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_stats_miss"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_stats_miss"

QID1="${CLICKHOUSE_DATABASE}_stats_miss_1"
QID2="${CLICKHOUSE_DATABASE}_stats_miss_2"

# Both queries prune on `b`, so each asks the part for `b`'s statistics.
for QID in "$QID1" "$QID2"; do
    ${CLICKHOUSE_CLIENT} --query_id="$QID" -q "
    SELECT count() FROM t_stats_miss WHERE b > 500
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0,
             enable_analyzer = 1, enable_parallel_replicas = 0
    FORMAT Null"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"

# The first query probes the corrupted blob once and logs; the second must find
# `b` negatively cached and emit nothing.
echo "first query warnings:"
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND query_id = '${QID1}'
  AND message LIKE '%while loading statistics for column b%'"
echo "second query warnings:"
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.text_log
WHERE event_date >= yesterday() AND query_id = '${QID2}'
  AND message LIKE '%while loading statistics for column b%'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_stats_miss SYNC"
