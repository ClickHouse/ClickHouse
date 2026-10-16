#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Searching a constant map materialized one copy of its keys and values per row, and the whole
# `rows * keys` scan then ran in a single function call with no cancellation checkpoint: the copy alone
# asked for 260 GiB at the 200k values and 200000 keys below, and `max_execution_time` was observed only
# after the call returned.
#
# `has(mapFromArrays(...), uid)` over a `set` skip index is what makes the single call part-sized: the
# index condition is one ExpressionActions run over every value the index stored for the part.
#
# `timeout_overflow_mode = 'break'` is what makes the first oracle exact instead of a timing threshold:
# in break mode `QueryStatus::checkTimeLimit` returns false rather than throwing and the skip index
# path discards that bool, so the only code that can raise an error here is a checkpoint inside the
# function, and its message names the function.
#
# `max_memory_usage` is pinned because the unfixed copy is what the deadline query would otherwise try
# to allocate; the cap turns that into an immediate error instead of 260 GiB of pressure.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_map_idx
    (
        type UInt32,
        uid LowCardinality(String),
        INDEX idx_uid uid TYPE set(10000) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY type
    -- pinned in the DDL: a granularity above the set's 10000 capacity would store far fewer values
    -- than there are rows, and it is one stored value per row that makes the scan long
    SETTINGS index_granularity = 1024;

    INSERT INTO t_map_idx SELECT 100500, toString(number % 10000) FROM numbers(200000);
    OPTIMIZE TABLE t_map_idx FINAL;
"

# Exactly one active part is required, not merely tidy: cancellation is already checked once per
# (part, index) before the work, so with two or more parts those checks interrupt the query on their
# own and the test would pass without the fix.
echo "active parts: $($CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_map_idx' AND active")"

# 200000 constant keys, none of them a uid value, so every stored value is compared against all of them:
# 4e10 comparisons, measured at 13.4s on a release build against the 3s deadline below. The margin is the
# point of the key count: at 50000 keys the same search takes 3.1s on a release build, close enough to the
# deadline that a faster machine would finish first and the oracle would stop asserting anything.
KEYS="arrayMap(x -> toString(x), range(1000000, 1200000))"

if timeout 120 $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM t_map_idx WHERE has(mapFromArrays($KEYS, range(200000)), uid)
        SETTINGS use_skip_indexes = 1,               -- the scan lives in skip index condition evaluation
                 use_skip_indexes_on_data_read = 1,
                 -- the oracle needs the index to be USED, not merely permitted: in break mode a query that
                 -- reads the data instead ends with no error text at all and this would go red on a fixed
                 -- build. Checked against the analysis-time useful_indices, so it holds either way.
                 force_data_skipping_indices = 'idx_uid',
                 -- bulk filtering evaluates a whole part in one condition call, so this needs the
                 -- periodic checkpoint and cannot be satisfied by the per-granule entry check
                 secondary_indices_enable_bulk_filtering = 1,
                 optimize_rewrite_has_to_in = 0,     -- keep the linear has(), do not rewrite it to a set
                 max_execution_time = 3, timeout_overflow_mode = 'break',
                 max_memory_usage = '4G'" 2>&1 \
    | grep -q "elapsed time limit reached in function has"
then
    echo "deadline: stopped in function has"
else
    echo "deadline: NOT stopped"
fi

# Second oracle, with no timing in it: the search must fit a small memory budget, as it already does for
# these keys spelled as a constant array. One key of the 2001 is a uid, so the count also shows that the
# rows and their results still line up. `max_block_size` is pinned because the copy was per block.
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM (SELECT toString(number % 10000) AS uid FROM numbers(65536))
    WHERE has(mapFromArrays(arrayPushBack(arrayMap(x -> toString(x), range(1000000, 1002000)), '7'),
                            range(2001)), uid)
    SETTINGS optimize_rewrite_has_to_in = 0, max_threads = 1, max_block_size = 65536,
             max_memory_usage = '500M'"
