#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-asan, no-tsan, no-msan, no-ubsan, no-debug
# ^^ this test pins memory limits around the aggregation-state footprint: sanitizer and debug builds distort memory accounting, and the fast-test runner is too small for the 13.8M-row fixture. no-parallel keeps the box quiet for the timing-sensitive capped runs.

# Regression test for the crash in in-order aggregation of `timeSeries*ToGrid` states under memory pressure.
# The old per-bucket `absl::flat_hash_map` storage was not exception-safe against its throwing
# `AllocatorWithMemoryTracking`: when MEMORY_LIMIT_EXCEEDED fired inside a bucket rehash during the in-order merge
# phase (`MergingAggregatedBucketTransform` -> `Aggregator::mergeBlocks`), the unwind destroyed a half-rehashed map
# and the process died in `free()` (SIGSEGV) instead of reporting the error. The sorted-append/packed sample buckets
# use strongly exception-safe containers, so the same query now either fits (the packed states are ~4x smaller than
# the raw ones, first query) or fails with a clean MEMORY_LIMIT_EXCEEDED (second query, a cap below even the packed
# floor, which exercises the throw-and-unwind path itself).
#
# The fixture reproduces the crash shape at 1/10 scale: 320 series x 43,140 samples (15s scrape over 7.5 days)
# aggregated to a 720-point grid (step 900, window 300) keeps 20 samples per bucket in every state; with the raw
# 16-byte samples in hash-map buckets the in-order merge phase needed >300MiB and segfaulted at the 220MiB cap
# (reproduced 6/6 on the pre-fix binary), while the packed states fit with ~1.7x headroom.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKDIR=${CLICKHOUSE_TMP}/04818_ts_in_order_${CLICKHOUSE_DATABASE}
rm -rf "${WORKDIR}"
mkdir -p "${WORKDIR}"

FIXTURE="
CREATE TABLE IF NOT EXISTS t (id UInt64, ts DateTime64(3), val Float64) ENGINE = MergeTree ORDER BY (id, ts);
INSERT INTO t SELECT intDiv(number, 43140) AS id, toDateTime64(1783344543 - 885 + (number % 43140) * 15, 3) AS ts, toFloat64(((number % 43140) * 3 + cityHash64(number) % 3) % 1000000) AS val FROM numbers_mt(13804800);
"

# count(), total defined grid points, and the rounded total rate mass: a compact deterministic fingerprint of all 320 grids.
FINGERPRINT="SELECT count(), sum(arrayCount(x -> isNotNull(x), v)), round(sum(arraySum(arrayMap(x -> ifNull(x, 0.), v))), 3) FROM (SELECT id, timeSeriesRateToGrid(toDateTime64(1783344543, 3), toDateTime64(1783991643, 3), toDecimal64(900, 3), toDecimal64(300, 3))(ts, val) AS v FROM t GROUP BY id)"

echo "in-order aggregation under the pre-fix crash cap matches plain hash aggregation:"
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --allow_experimental_time_series_aggregate_functions=1 -q "
${FIXTURE}
${FINGERPRINT}
SETTINGS max_threads = 16, max_memory_usage = 220000000, max_bytes_ratio_before_external_group_by = 0, max_bytes_before_external_group_by = 0, optimize_aggregation_in_order = 1;
${FINGERPRINT}
SETTINGS max_threads = 16, optimize_aggregation_in_order = 0;
"

echo "a cap below the packed floor fails with a clean error:"
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --allow_experimental_time_series_aggregate_functions=1 -q "
${FINGERPRINT}
SETTINGS max_threads = 16, max_memory_usage = 80000000, max_bytes_ratio_before_external_group_by = 0, max_bytes_before_external_group_by = 0, optimize_aggregation_in_order = 1;
" 2>&1 | grep -o -m1 'MEMORY_LIMIT_EXCEEDED'

rm -rf "${WORKDIR}"
