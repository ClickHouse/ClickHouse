#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The guard in `05217_aggregation_in_order_fixed_keys` is only a guard while its `GROUP BY a, b` keeps
# selecting a batch-packing method: with a method that packs row by row there is no quadratic term to
# guard against and the timing check would pass by doing nothing. Pin the method for those key types,
# and prove the check discriminates by pinning a wider key that no longer batch-packs (`keys256`
# leaves the batching class through `sizeof(Key) > 16`).
#
# The in-order paths (`executeOnBlockSmall`, `mergeOnBlockSmall`) do not log a method, so the shape
# here is the whole-block one; `Aggregator::method_chosen_for_in_order` is derived from the same
# `method_chosen` and only rewrites the `prealloc_serialized` variants. What is pinned is therefore the
# map variant `keys128`, while 05217's guards also reach its `*_void` variant, the
# `ClearableSetVariants` one DISTINCT in order uses and the merging path - the pinned property is the
# one all of them share: two non-nullable 8-byte keys pack into a 16-byte key.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_method;
    CREATE TABLE t_method (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_method SELECT number, number * 7, number * 11 FROM numbers(65536);
"

function aggregation_method()
{
    $CLICKHOUSE_CLIENT --send_logs_level=trace --optimize_aggregation_in_order=0 --max_threads=1 \
        --group_by_two_level_threshold=0 --group_by_two_level_threshold_bytes=0 \
        -q "SELECT $1, count() FROM t_method GROUP BY $1 FORMAT Null" 2>&1 \
        | grep -oE 'Aggregation method: [a-z_0-9]+' | sort -u
}

# The keys of the guarded query: two UInt64 columns, 16 bytes, batch-packed on state construction.
aggregation_method "a, b"
# One column wider: the same shape stops batch-packing.
aggregation_method "a, b, c"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_method"
