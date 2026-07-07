#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --automatic_parallel_replicas_mode 0 --enable_parallel_replicas 0 --use_statistics_for_part_pruning 0 --use_skip_indexes 0 --merge_tree_min_rows_for_seek 0 --merge_tree_min_bytes_for_seek 0 --use_lightweight_primary_key_index_analysis 1"

# The leading key column is unique per row, so with
# primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9 the
# trailing key column `b` is trimmed from the in-memory primary index.
# A filter on `b` alone then provably cannot exclude any granule, and the
# generic exclusion search is short-circuited.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_trimmed;
CREATE TABLE t_trimmed (a UInt64, b UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 16, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.9;
INSERT INTO t_trimmed SELECT number, cityHash64(number) % 1000, cityHash64(number, 1) % 100 FROM numbers(10000);"

# The same table without index trimming: the filter column has per-mark values,
# the exclusion search must actually run.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_full;
CREATE TABLE t_full (a UInt64, b UInt32, v UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 16, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;
INSERT INTO t_full SELECT number, cityHash64(number) % 1000, cityHash64(number, 1) % 100 FROM numbers(10000);"

echo "-- results are identical with and without the trimmed index"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_trimmed WHERE b = 42" --query_id="${query_prefix}_trimmed_1eq"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_full WHERE b = 42"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_trimmed WHERE b IN (42, 555, 999)" --query_id="${query_prefix}_trimmed_2in"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_full WHERE b IN (42, 555, 999)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_trimmed WHERE b NOT IN (42, 555, 999)" --query_id="${query_prefix}_trimmed_3notin"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_full WHERE b NOT IN (42, 555, 999)"

echo "-- the short-circuit does not change the selected granules"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_trimmed WHERE b = 42" | grep -E 'PrimaryKey|Search Algorithm|Parts:|Granules:' | sed 's/^ *//'

# A filter on the loaded leading key column must still prune normally.
echo "-- leading-key filters still prune"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_trimmed WHERE a = 77" --query_id="${query_prefix}_trimmed_4lead"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t_full WHERE a = 77"
$CLICKHOUSE_CLIENT -q "EXPLAIN indexes = 1 SELECT count() FROM t_trimmed WHERE a = 77" | grep -E 'Parts:|Granules:' | sed 's/^ *//'

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo "-- short-circuit fires exactly for trailing-key filters on the trimmed index"
$CLICKHOUSE_CLIENT -q "
SELECT
    splitByString('${query_prefix}_trimmed_', query_id)[2],
    ProfileEvents['IndexGenericExclusionSearchShortCircuit'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id LIKE '${query_prefix}\\_trimmed\\_%'
ORDER BY query_id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_trimmed; DROP TABLE t_full"
