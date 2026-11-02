#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-header-constants.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Expanding either header constant for one input block requires 512 MiB, exceeding the query limit.
# Checksums depend on both the key and payload, keeping the payload in the plan after filtering.
for payload_type in string array; do
    if [[ ${payload_type} == string ]]; then
        payload="repeat('x', 65536)"
        checksum="cityHash64(k, payload)"
    else
        payload="range(toUInt64(8192))"
        # Array element lookups cover every stored value without expanding the constant.
        checksum="cityHash64(k, payload[k + 1], payload[k + 4097])"
    fi
    for threshold in 0 1099511627776 1; do
        for ordered in 0 1; do
            order_by=""
            check_order="1"
            if [[ ${ordered} == 1 ]]; then
                # Ordering by an expression requires arrival-order restoration after the spill merge.
                order_by="ORDER BY k + 1 DESC"
                check_order="groupArray(k) = arrayReverseSort(groupArray(k))"
            fi

            ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
                SELECT count(), sum(${checksum}), ${check_order}
                FROM
                (
                    SELECT DISTINCT number % 4096 AS k, ${payload} AS payload
                    FROM numbers(32768)
                    ${order_by}
                )
                SETTINGS max_threads = 1, max_block_size = 8192,
                    max_memory_usage = 134217728, max_untracked_memory = 0,
                    max_bytes_ratio_before_external_distinct = 0,
                    max_bytes_before_external_distinct = ${threshold},
                    optimize_distinct_in_order = 0, count_distinct_optimization = 0;

                SELECT sumIf(value, event = 'ExternalDistinctWritePart') > 0,
                       sumIf(value, event = 'ExternalDistinctMerge') > 0
                FROM system.events;"
        done
    done
done

# The set grows across chunks before spilling. Rows emitted during hashing and rows restored from
# ordinary runs must carry the same constant, while suppression runs prevent duplicate output.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
    SELECT count(), sum(cityHash64(k, payload))
    FROM
    (
        SELECT DISTINCT number % 1048576 AS k, repeat('x', 1024) AS payload
        FROM numbers(2097152)
    )
    SETTINGS max_threads = 1, max_block_size = 8192,
        max_memory_usage = 134217728, max_untracked_memory = 0,
        max_bytes_ratio_before_external_distinct = 0,
        max_bytes_before_external_distinct = 50331648,
        optimize_distinct_in_order = 0, count_distinct_optimization = 0;

    SELECT sumIf(value, event = 'ExternalDistinctWritePart') > 0,
           sumIf(value, event = 'ExternalDistinctMerge') > 0
    FROM system.events;"
