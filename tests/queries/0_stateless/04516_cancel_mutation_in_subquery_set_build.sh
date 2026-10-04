#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/51586
#
# A mutation whose `x IN (subquery)` right-hand side is materialized during
# primary-key analysis (`KeyCondition::buildOrderedSetInplace`) used to run the
# set-building pipeline without observing mutation cancellation. A large or slow
# subquery therefore blocked server shutdown / `KILL MUTATION` until the whole
# subquery had finished. The set build now polls the mutation's cancellation
# state, so cancelling the mutation stops it promptly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cancel_in_set_build"

# `auto_statistics_types = ''` keeps the set from being built via the statistics
# estimation path, so the mutation exercises the key-analysis build under test.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_cancel_in_set_build (key Int) ENGINE = MergeTree ORDER BY key
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = '';
    INSERT INTO t_cancel_in_set_build SELECT number FROM numbers(100);"

# Queue a mutation whose IN-subquery set is slow to build: the subquery sleeps
# while producing rows, so building the set for `key IN (...)` takes minutes
# unless the build observes cancellation.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_cancel_in_set_build DELETE WHERE key IN (
        SELECT number FROM numbers(10000000) WHERE sleep(1) = 0
    ) SETTINGS mutations_sync = 0"

# Wait until the mutation has actually been running for a couple of seconds,
# i.e. it is inside the set-building pipeline and not merely queued. Fail hard on
# timeout: if the mutation never reaches this running `system.merges` state, then
# `KILL MUTATION` below would cancel a still-queued mutation and the final `0` /
# `100` output would match the reference without ever exercising the in-flight
# `CompletedPipelineExecutor` cancellation path this test is meant to cover.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_in_set_build' AND is_mutation AND elapsed > 2")" -ne 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Mutation did not start in time" >&2
        exit 1
    fi
done

# Cancel the mutation. The in-flight set build must stop promptly.
$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_cancel_in_set_build' FORMAT Null"

# The background mutation must disappear quickly. Without the fix its merge-list
# entry lingers until the whole subquery finishes (minutes).
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_in_set_build'")" -ne 0 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 100 ]; then
        break
    fi
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_in_set_build'"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_cancel_in_set_build"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_cancel_in_set_build"
