#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-ordered-workspace.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The one-byte threshold starts spilling before any hash insertion. The large input block needs
# arrival numbers alongside the sorting workspace, and readback must restore descending input order.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
    SELECT count(), sum(k), groupArray(k) = arrayReverseSort(groupArray(k))
    FROM
    (
        SELECT DISTINCT number AS k
        FROM numbers(262144)
        ORDER BY k + 1 DESC
    )
    SETTINGS max_threads = 1, max_block_size = 262144,
        max_memory_usage = 67108864, max_untracked_memory = 0,
        max_bytes_before_external_distinct = 1, max_bytes_ratio_before_external_distinct = 0,
        optimize_distinct_in_order = 0;

    SELECT sumIf(value, event = 'ExternalDistinctWritePart') > 0,
           sumIf(value, event = 'ExternalDistinctMerge') > 0
    FROM system.events;"
