#!/usr/bin/env bash

# A LEFT JOIN whose right table has unique keys is promoted to RightAny. With a non-equi ON
# condition the join processes only a prefix of the left block once max_joined_block_size_rows
# candidate rows are collected, so the block is scattered to that prefix while the negated null
# map `filter` kept the full block size. Applying that stale filter to the shorter left key
# column raised a size-mismatch LOGICAL_ERROR.
# Here `k = 0` fails `k > b` and `k = 1` matches.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Swapping the tables turns this into a RIGHT join, which is not promoted to RightAny and takes
# the need_replication path that always resized `filter`, so neither query below would reproduce.
# The query cache is off because the same query runs more than once here, and serving it from the
# cache skips the join, which the assertions at the end would read as the join having changed.
SETTINGS="SETTINGS enable_analyzer = 1, query_plan_join_swap_table = false, use_query_cache = 0, join_algorithm = 'hash', max_joined_block_size_rows = 1"

# Raised "Null map of size 2 at offset 0 does not match ColumnNullable of size 1".
$CLICKHOUSE_CLIENT -q "
    SELECT l.k, r.a
    FROM (SELECT number AS k FROM numbers(2)) AS l
    LEFT JOIN (SELECT toNullable(number) AS a, 0 AS b FROM numbers(2)) AS r
      ON (l.k = r.a) AND (l.k > r.b)
    ORDER BY l.k
    $SETTINGS, join_use_nulls = 1"

# Same shape with a non-nullable right key hit the sibling "Invalid number of rows in Chunk".
$CLICKHOUSE_CLIENT -q "
    SELECT l.k, r.a
    FROM (SELECT number AS k FROM numbers(2)) AS l
    LEFT JOIN (SELECT number AS a, 0 AS b FROM numbers(2)) AS r
      ON (l.k = r.a) AND (l.k > r.b)
    ORDER BY l.k
    $SETTINGS, join_use_nulls = 0"

# The results above are the same whether or not the two conditions the bug needs hold, so assert
# both of them, otherwise the queries can silently stop covering the fixed path.

# 1. The strictness really was promoted to RightAny. Without it the join stays Left ALL, which
#    has need_replication and was already resized by the guard this fixes.
# $CLICKHOUSE_CLIENT already carries --send_logs_level, and passing it twice is rejected, so
# replace it rather than appending another one.
CLICKHOUSE_CLIENT_DEBUG=$(echo "$CLICKHOUSE_CLIENT" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=debug/")
$CLICKHOUSE_CLIENT_DEBUG -q "
    SELECT l.k, r.a
    FROM (SELECT number AS k FROM numbers(2)) AS l
    LEFT JOIN (SELECT toNullable(number) AS a, 0 AS b FROM numbers(2)) AS r
      ON (l.k = r.a) AND (l.k > r.b)
    ORDER BY l.k
    $SETTINGS, join_use_nulls = 1" 2>&1 \
  | grep -c -m1 'Promoting join strictness to RightAny' \
  | sed 's/^1$/promoted/;s/^0$/NOT PROMOTED/'

# 2. The block really was split. The deferred left row is probed again in the following block,
#    so 2 left rows are probed 3 times; without the split it is 2.
$CLICKHOUSE_CLIENT --query_id="04612_split_$CLICKHOUSE_DATABASE" -q "
    SELECT l.k, r.a
    FROM (SELECT number AS k FROM numbers(2)) AS l
    LEFT JOIN (SELECT toNullable(number) AS a, 0 AS b FROM numbers(2)) AS r
      ON (l.k = r.a) AND (l.k > r.b)
    ORDER BY l.k
    $SETTINGS, join_use_nulls = 1" > /dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT if(probed = 3, 'split', format('NOT SPLIT: {}', probed))
    FROM (
        SELECT argMax(ProfileEvents['JoinProbeTableRowCount'], event_time_microseconds) AS probed
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id = '04612_split_$CLICKHOUSE_DATABASE'
    )
    SETTINGS use_query_cache = 0"
