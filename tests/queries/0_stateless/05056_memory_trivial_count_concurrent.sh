#!/usr/bin/env bash
# Tags: long

# `SELECT count()` on a `Memory` table is served from the row counter of the table. The counter is
# published together with the blocks it describes, so a concurrent reader always observes the exact
# number of rows of one of the committed states of the table - never a value that belongs to no
# state at all. Every insert here adds exactly 1000 rows and every mutation deletes exactly 1000
# rows, so every observable count is a multiple of 1000.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_memory_count_concurrent;
    CREATE TABLE t_memory_count_concurrent (k UInt64) ENGINE = Memory;
"

writer()
{
    for i in {1..20}; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO t_memory_count_concurrent SELECT number + ${i}000000 FROM numbers(1000)"
    done
}

mutator()
{
    for i in {1..8}; do
        $CLICKHOUSE_CLIENT --query "ALTER TABLE t_memory_count_concurrent DELETE WHERE k >= ${i}000000 AND k < ${i}000000 + 1000"
    done
}

reader()
{
    for _ in {1..40}; do
        $CLICKHOUSE_CLIENT --query "
            SELECT throwIf(count() % 1000 != 0, 'Row count of a Memory table does not correspond to any of its states')
            FROM t_memory_count_concurrent
            SETTINGS optimize_trivial_count_query = 1"
    done
}

writer &
mutator &
reader &
wait

# When quiescent, the count served from the metadata must agree with the count from the read path.
$CLICKHOUSE_CLIENT --query "
    SELECT
        (SELECT count() FROM t_memory_count_concurrent SETTINGS optimize_trivial_count_query = 1)
        = (SELECT count() FROM t_memory_count_concurrent SETTINGS optimize_trivial_count_query = 0);
    DROP TABLE t_memory_count_concurrent;
"
