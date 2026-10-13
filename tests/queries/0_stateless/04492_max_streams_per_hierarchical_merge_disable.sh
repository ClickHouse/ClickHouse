#!/usr/bin/env bash

# Regression test: `max_streams_per_hierarchical_merge = 0` must keep the final
# merge after a full sort flat. With the default value, a layer of parallel
# mergers must appear once there are more input streams than the threshold.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `grep -c "MergingSortedTransform ×"` is 1 only when a layer of parallel
# mergers exists, i.e. when the hierarchical merge was built.
has_hierarchy() { grep -c "MergingSortedTransform ×"; }

# `max_threads` is pinned so the CI randomizer cannot set it to 1, which would
# remove multi-stream merging entirely and make even the enabled case flat.
# `max_threads_min_free_memory_per_thread = 0` disables the free-memory limiter in
# `getMaxThreadsForAvailableMemory`: with the default `1_GiB` a memory-limited runner
# would silently lower `max_threads` below the threshold, so the hierarchy (which needs
# more streams than the threshold) would not appear and the enabled checks would flip to 0.
# `02343_aggregation_pipeline.sql` disables the same limiter for stable `EXPLAIN PIPELINE`.

# disabled: single flat merge (0), default 16: hierarchy over 32 streams (1)
# `max_rows_to_read = 0` overrides the stateless test profile default (20M), which
# `EXPLAIN PIPELINE` would otherwise trip while building the `numbers_mt(100000000)` pipe.
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT number FROM numbers_mt(100000000) ORDER BY number
    SETTINGS max_threads = 32, max_threads_min_free_memory_per_thread = 0, max_streams_per_hierarchical_merge = 0, max_rows_to_read = 0" | has_hierarchy
$CLICKHOUSE_CLIENT -q "EXPLAIN PIPELINE SELECT number FROM numbers_mt(100000000) ORDER BY number
    SETTINGS max_threads = 32, max_threads_min_free_memory_per_thread = 0, max_streams_per_hierarchical_merge = 16, max_rows_to_read = 0" | has_hierarchy
