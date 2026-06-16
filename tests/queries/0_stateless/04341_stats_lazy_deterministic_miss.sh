#!/usr/bin/env bash
# Tags: no-object-storage
#
# `no-object-storage`: the test corrupts a statistics file directly inside the
# part directory on the local disk. On the s3 / azure object-storage disks the
# files in that directory are `DiskObjectStorageMetadata` pointer files, not the
# statistics payload, so overwriting them does not corrupt the stored statistics.
#
# Regression test for the deterministic-miss negative cache in
# `IMergeTreeDataPart::getEstimates`. A column whose statistics are declared in
# the table metadata but whose statistics file is unreadable (here: corrupted)
# must be probed at most once per in-memory part object. Before the fix, only
# columns with *no* declared statistics were recorded in
# `estimates_attempted_columns`, so a metadata-declared column with a broken
# statistics file was re-read (and re-warned) on every query.
# Related: https://github.com/ClickHouse/ClickHouse/pull/104691

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_stats_miss SYNC"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_stats_miss (a UInt64, b UInt64 STATISTICS(minmax))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, materialize_statistics_on_insert = 1"

${CLICKHOUSE_CLIENT} -q "INSERT INTO t_stats_miss SELECT number, number FROM numbers(1000)"

DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND table = 't_stats_miss'")
PART=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_stats_miss' AND active LIMIT 1")

# Corrupt the statistics file for column `b` so deserialization deterministically
# fails: a leading version header of all-zero bytes is an unsupported version. The
# size is preserved so the part's on-load size check (`Checksum::checkSize`, which
# does not verify content) still passes and the part is not marked broken.
STATS_FILE="${DATA_PATH}${PART}/statistics_b.stats"
ORIG_SIZE=$(stat -c %s "$STATS_FILE")
head -c "$ORIG_SIZE" /dev/zero > "$STATS_FILE"

# Drop the per-part estimates cache populated during INSERT and rebuild the part
# object from disk so the SELECTs below load statistics for real.
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_stats_miss"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_stats_miss"

QID1="${CLICKHOUSE_DATABASE}_stats_miss_1"
QID2="${CLICKHOUSE_DATABASE}_stats_miss_2"

# Both queries prune on `b`, so each one asks the part for `b`'s statistics.
for QID in "$QID1" "$QID2"; do
    ${CLICKHOUSE_CLIENT} --query_id="$QID" -q "
    SELECT count() FROM t_stats_miss WHERE b > 500
    SETTINGS use_statistics_for_part_pruning = 1, use_statistics = 0,
             use_statistics_cache = 0, enable_analyzer = 1, enable_parallel_replicas = 0
    FORMAT Null"
done

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"

# The first query probes the corrupted file once and logs a warning; the second
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
