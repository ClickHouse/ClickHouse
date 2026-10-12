#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
# no-random-merge-tree-settings, no-random-settings: to stabilize the collected statistics

# The automatic parallel replicas statistics collector derives the compression ratio of the aggregate-state
# columns produced by `AggregatingInOrderTransform` from a sample it pushes through a `CompressedWriteBuffer`.
# A block that holds only a handful of tiny states - `count()` states are a single varint each - produces a
# sample smaller than the compressed format's per-block framing (checksum plus header), so the "compressed"
# sample comes out larger than the sample itself. Such a sample must be reported as incompressible, not as
# expanding: a ratio below one inflates the `AggregationState` estimate above the uncompressed size and can
# wrongly disable automatic parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_tiny_states"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_tiny_states (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 8192, auto_statistics_types = ''"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_tiny_states SELECT number FROM numbers(20000)"

# `max_block_size = 8` makes `AggregatingInOrderTransform` emit eight `count()` states per block, i.e. about
# eight bytes of payload against twenty five bytes of framing.
$CLICKHOUSE_CLIENT --send_logs_level=test -q "
    SELECT key, count() FROM t_tiny_states GROUP BY key FORMAT Null
    SETTINGS
        enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
        parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'parallel_replicas',
        enable_analyzer = 1, optimize_aggregation_in_order = 1, max_threads = 1, max_block_size = 8,
        use_uncompressed_cache = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0
    " 2>&1 \
    | grep -o 'AggregationState bytes=[0-9]*, sample_bytes=[0-9]*, compressed_bytes=[0-9]*' \
    | sed -E 's/.*sample_bytes=([0-9]+), compressed_bytes=([0-9]+)/\1 \2/' \
    | while read -r sample compressed
      do
          if [ "$compressed" -le "$sample" ]
          then
              echo "AggregationState sample is not reported as expanding"
          else
              echo "FAIL: sample_bytes=$sample compressed_bytes=$compressed"
          fi
      done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_tiny_states"
