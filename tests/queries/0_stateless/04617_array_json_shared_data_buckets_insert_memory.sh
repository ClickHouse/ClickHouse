#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-msan, no-tsan, no-ubsan, no-random-settings
# Checks that the bucketed shared-data serializations (map_with_buckets and advanced) split the shared
# data one bucket at a time on write (via SharedDataBucketsSplitter) instead of materializing all
# buckets at once. A single-row Array(JSON) with 1000 nested elements (~500 MB) packs many nested rows
# into one granule, so the bucket split is large: without the optimization the insert needs >1.1 GiB,
# with it ~630 MiB. We cap the insert at 800 MiB via max_memory_usage, so a regression throws.
# Sanitizers inflate memory and randomized settings change the buffers/limits, so those are excluded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One row whose array holds 1000 JSON objects, each with 1000 fields of 500-char string values
# (~500 KB per element, ~500 MB total). With max_dynamic_paths=0 every field overflows into shared data.
GEN="arrayMap(i -> CAST(concat('{', arrayStringConcat(arrayMap(j -> concat('\"f', toString(j), '\":\"', repeat('x', 500), '\"'), range(1000)), ','), '}'), 'JSON(max_dynamic_paths=0)'), range(1000)) AS data"

for VERSION in map_with_buckets advanced; do
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_arr_json_buckets"
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE t_arr_json_buckets (data Array(JSON(max_dynamic_paths=0)))
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS object_shared_data_serialization_version_for_zero_level_parts = '${VERSION}'"

    # 800 MiB: comfortably above the ~630 MiB optimized peak, well below the >1.1 GiB an unoptimized
    # (all-buckets-at-once) split would need, so a regression fails with MEMORY_LIMIT_EXCEEDED.
    ${CLICKHOUSE_CLIENT} --max_memory_usage 838860800 -q "INSERT INTO t_arr_json_buckets SELECT ${GEN}"

    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_arr_json_buckets"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE t_arr_json_buckets"
done
