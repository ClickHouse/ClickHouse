#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A threshold flush of a `Buffer` table writes out a block that can hold rows buffered by several
# queries, and its pre-write decision has to be shared with each of them: a query whose rows were
# flushed by another query's `INSERT` must not re-run the `Too many parts` check later and count the
# part that flush created from its own rows.
#
# Query B buffers its first block and writes its second one 2.4 seconds later. In between, query A
# appends to the buffer, and doing so flushes B's buffered block to the destination, creating a part.
# B's second block exceeds the buffer thresholds and is written to the destination directly - with
# `parts_to_throw_insert = 1` that nested `INSERT` used to run the check, count the part holding B's
# own rows, and reject B in the middle of the query. The interleaving is timing-dependent: when it is
# not reached, the test degenerates to orderings that pass both before and after the fix, so it can
# not fail spuriously.

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_05054_dst;
    DROP TABLE IF EXISTS t_05054_buf;

    CREATE TABLE t_05054_dst (n UInt64) ENGINE = MergeTree ORDER BY n
        SETTINGS parts_to_throw_insert = 1;

    -- Two rows fit into the buffer; appending a block that would push it over max_rows = 2 first
    -- flushes the buffered data, and a block of more than two rows skips the buffer entirely.
    CREATE TABLE t_05054_buf (n UInt64)
        ENGINE = Buffer(currentDatabase(), t_05054_dst, 1, 1000000, 1000000, 1000000, 2, 1000000000, 1000000000);
"

# Query B: the first branch produces a single row at once, the second one a single three-row block
# after 2.4 seconds of sleeping.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO t_05054_buf
    SELECT number FROM numbers(1)
    UNION ALL
    SELECT number + 10 + ignore(sleepEachRow(0.8)) FROM numbers(3)
    SETTINGS max_block_size = 65536, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1, max_threads = 2
" &
query_b_pid=$!

# Wait until B's first block sits in the buffer, so that query A below is the one to flush it.
for _ in {1..600}
do
    [ "$(${CLICKHOUSE_CLIENT} -q 'SELECT count() FROM t_05054_buf')" = "1" ] && break
    sleep 0.05
done

# Query A: two rows are buffered, and appending them first flushes B's block to the destination.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_05054_buf SELECT number + 20 FROM numbers(2) SETTINGS async_insert = 0"

# B has to succeed: the flush of its first block already made its pre-write decision for it.
wait $query_b_pid

# All four of B's rows reach the destination; A's two rows are still buffered.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM t_05054_dst;
    SELECT count() FROM t_05054_buf;

    -- Let the buffered rows flush on DROP without tripping the limit.
    ALTER TABLE t_05054_dst MODIFY SETTING parts_to_throw_insert = 1000;

    DROP TABLE t_05054_buf;
    DROP TABLE t_05054_dst;
"
