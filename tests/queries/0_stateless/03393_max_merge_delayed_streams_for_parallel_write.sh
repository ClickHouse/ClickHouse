#!/usr/bin/env bash
# Tags: no-fasttest, long, no-parallel, no-flaky-check, no-msan
# - no-fasttest -- S3 is required
# - no-flaky-check -- not compatible with ThreadFuzzer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The merge of a 1200+ column `metric_log` on `s3_no_cache` storage is intrinsically slow
# under sanitizers (TSan in particular) and can exceed the default 300s *client* receive
# timeout, producing a transient `TIMEOUT_EXCEEDED` in `ClientBase::receiveResult`. That
# timeout is read from `connection_parameters.timeouts.receive_timeout`, which is built
# from the `--receive_timeout` CLI option of `clickhouse-client` (see
# `src/Client/ConnectionParameters.cpp`); it is *not* derived from a per-query server-side
# `SETTINGS receive_timeout`. So we run the two `OPTIMIZE FINAL` statements through
# `${CLICKHOUSE_CLIENT} --receive_timeout=900 ...` while keeping the rest of the test
# untouched. The merge itself is healthy and continues on the server; the longer client
# timeout only avoids the client giving up before the wide-column merge completes.

${CLICKHOUSE_CLIENT} -m -q "
SET optimize_trivial_insert_select = 0;

SYSTEM FLUSH LOGS system.metric_log;

CREATE TABLE metric_log AS system.metric_log
ENGINE = MergeTree
PARTITION BY ()
ORDER BY ()
SETTINGS
    -- cache has its own problems (see filesystem_cache_prefer_bigger_buffer_size)
    storage_policy = 's3_no_cache',
    -- horizontal merges open all streams at once, so will still use huge amount of memory
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_full_part_storage = 0,
    -- avoid excessive memory usage (default buffer size of 1MiB per column)
    max_merge_delayed_streams_for_parallel_write = 100,
    -- avoid superfluous merges
    merge_selector_base = 1000,
    min_columns_to_activate_adaptive_write_buffer = 0,
    auto_statistics_types = '';

INSERT INTO metric_log SELECT * FROM generateRandom() LIMIT 10;
"

${CLICKHOUSE_CLIENT} --receive_timeout=900 -q "OPTIMIZE TABLE metric_log FINAL"

${CLICKHOUSE_CLIENT} -m -q "
SYSTEM FLUSH LOGS part_log;
SELECT 'max_merge_delayed_streams_for_parallel_write=100' AS test, *
FROM system.part_log
WHERE table = 'metric_log'
  AND database = currentDatabase()
  AND event_date >= yesterday()
  AND event_time >= now() - 600
  AND event_type = 'MergeParts'
  AND peak_memory_usage > 1_000_000_000
FORMAT Vertical;

ALTER TABLE metric_log MODIFY SETTING max_merge_delayed_streams_for_parallel_write = 10000;
"

${CLICKHOUSE_CLIENT} --receive_timeout=900 -q "OPTIMIZE TABLE metric_log FINAL"

${CLICKHOUSE_CLIENT} -m -q "
SYSTEM FLUSH LOGS part_log;
SELECT 'max_merge_delayed_streams_for_parallel_write=1000' AS test, count() AS count
FROM system.part_log
WHERE table = 'metric_log'
  AND database = currentDatabase()
  AND event_date >= yesterday()
  AND event_time >= now() - 600
  AND event_type = 'MergeParts'
  AND peak_memory_usage > 1_000_000_000;
"
