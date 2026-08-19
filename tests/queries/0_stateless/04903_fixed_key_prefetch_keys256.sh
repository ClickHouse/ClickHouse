#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="
    SELECT k, count()
    FROM
    (
        SELECT toUInt256(number % 3) AS k
        FROM numbers(100000)
    )
    GROUP BY k
    ORDER BY k
    SETTINGS enable_software_prefetch_in_aggregation = 1"

prefetch_query="
    SELECT count()
    FROM
    (
        SELECT toUInt256(number) AS k
        FROM numbers(100000)
    )
    GROUP BY k
    FORMAT Null
    SETTINGS enable_software_prefetch_in_aggregation = 1"

# This event is incremented only after `HashMethodKeysFixed` computes hashes
# from its batch-packed `prepared_keys`, so it proves the `UInt256` path added
# by this change rather than merely the pre-existing `keys256` method choice.
$CLICKHOUSE_CLIENT --profile-events-delay-ms=-1 --print-profile-events -q "$prefetch_query" 2>&1 \
    | grep -qE 'AggregationPrecomputedFixedKeyHashes: [1-9][0-9]*' && echo 1
$CLICKHOUSE_CLIENT -q "$query"

# With prefetch disabled, `keys256` uses the per-row encoder. Its layout must still
# match the size-sorted layout used while materializing aggregate keys.
$CLICKHOUSE_CLIENT -q "
    SELECT k8, k128, count()
    FROM
    (
        SELECT toUInt8(number % 2) AS k8, toUInt128(number % 3) AS k128
        FROM numbers(100000)
    )
    GROUP BY k8, k128
    ORDER BY k8, k128
    SETTINGS enable_software_prefetch_in_aggregation = 0"
