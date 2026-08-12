#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cancel_minmax_set_build"

# A partition key makes the minmax_count projection eligible, and `auto_statistics_types = ''` keeps
# the set from being built via the statistics estimation path.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_cancel_minmax_set_build (a Int32, b Int32)
    ENGINE = MergeTree ORDER BY a PARTITION BY a % 3
    SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 0, auto_statistics_types = '';
    INSERT INTO t_cancel_minmax_set_build SELECT number, number + 1 FROM numbers(15);"

# The minmax_count projection must actually be chosen for the predicate shape under test, otherwise
# the cancellation below never reaches the projection's synchronous filter evaluation. The projection
# settings are pinned here because the test runner randomizes them, which would otherwise make this
# assertion report a plan that the mutation below does not use.
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM (
        EXPLAIN indexes = 1
        SELECT count() FROM t_cancel_minmax_set_build WHERE 1 IN (SELECT number FROM numbers(3))
        SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1
    ) WHERE explain ILIKE '%_minmax_count_projection%'"

# A *constant* left-hand side is essential: it maps to no key column, so primary-key analysis returns
# before building the set, and the projection's filter evaluation is the first materialization attempt.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_cancel_minmax_set_build UPDATE b = 0 WHERE 1 IN (
        SELECT number FROM numbers(10000000) WHERE sleep(1) = 0
    ) SETTINGS mutations_sync = 0"

# Wait until a mutation has been running for a couple of seconds, i.e. it is inside the set-building
# pipeline rather than merely queued. Cancelling a queued mutation would produce the same final output
# without ever exercising the in-flight cancellation path under test, so time out loudly instead.
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_minmax_set_build' AND is_mutation AND elapsed > 2")" -lt 1 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Mutation did not start in time" >&2
        exit 1
    fi
done

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_cancel_minmax_set_build' FORMAT Null"

# The server must survive the cancellation. Without the fix the cancelled set build stopped without
# reporting, and the projection then evaluated a filter holding an unbuilt set, which aborts a
# debug/sanitizer build with "Not-ready Set is passed as the second argument for function 'in'".
i=0
while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_minmax_set_build'")" -ne 0 ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 100 ]; then
        break
    fi
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_cancel_minmax_set_build'"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(b) FROM t_cancel_minmax_set_build"

# A cancelled mutation records ABORTED for the part it was mutating. The rows above survive a build
# that only catches the unbuilt-set error rather than aborting on it, so this is the assertion that
# pins the reporting itself. The part log entry is queued asynchronously, hence the wait.
aborted_in_part_log="
    SELECT countIf(errorCodeToName(error) = 'ABORTED') > 0
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND table = 't_cancel_minmax_set_build'
      AND event_type = 'MutatePart' AND error != 0"
i=0
while [ "$($CLICKHOUSE_CLIENT -m -q "SYSTEM FLUSH LOGS part_log; $aborted_in_part_log")" != "1" ]; do
    sleep 0.3
    i=$((i + 1))
    if [ "$i" -gt 200 ]; then
        echo "Cancelled mutation did not record ABORTED in system.part_log" >&2
        exit 1
    fi
done

# A mutation that is not cancelled must still complete, and the cancelled one must not have left the
# table unmutatable.
$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t_cancel_minmax_set_build UPDATE b = 7 WHERE 1 IN (SELECT number FROM numbers(3))
    SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT b FROM t_cancel_minmax_set_build"

$CLICKHOUSE_CLIENT -q "$aborted_in_part_log"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_cancel_minmax_set_build"
