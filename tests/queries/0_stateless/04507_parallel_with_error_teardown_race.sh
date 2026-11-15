#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for a teardown/lifetime data race in InterpreterParallelWithQuery:
# an exception raised while subqueries are still being enqueued used to destroy the
# interpreter (and its worker-shared io_holders / combined_pipeline / mutex) before the
# worker threads that touch them were joined. Repeatedly driving the error path with
# max_threads > 1 exercises that teardown; the server must stay up (checked under TSan/ASan).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "CREATE TABLE t (x UInt64) ENGINE = MergeTree() ORDER BY x"

# One subquery inserts a real table, the other references a missing table so an exception is
# thrown while the pool still has a live worker. max_threads > 1 forces the thread-pool path.
for _ in {1..30}; do
    $CLICKHOUSE_CLIENT --max_threads 4 --query "
        INSERT INTO t SELECT number FROM numbers(4)
        PARALLEL WITH
        INSERT INTO nonexistent_table_04507 SELECT number FROM numbers(4)
    " >/dev/null 2>&1
done

# The server must still be alive and answering after the error/teardown churn.
$CLICKHOUSE_CLIENT --query "SELECT 'ok'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
