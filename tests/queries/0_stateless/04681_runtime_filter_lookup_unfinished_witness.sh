#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: the fail point is process-global, so while it is armed any concurrent query
#   also builds a runtime filter that never finishes.
# - no-fasttest: fail points need a build with libfiu.

# The fail point holds a registered runtime filter in the state it otherwise only passes
# through transiently: findable by the probe side while inserts_are_finished is still false.
# ProfileEvents make the branch taken inside IRuntimeFilter::find() observable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The disable must run on a path that survives a failing query: the witness query below aborts
# a binary without the guard, and a process-global fail point left armed changes the behaviour
# of every later test on the same server. Disabling twice is a no-op.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT runtime_filter_skip_finish_insert" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rf_witness_probe;
    DROP TABLE IF EXISTS rf_witness_build;

    CREATE TABLE rf_witness_probe (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
    CREATE TABLE rf_witness_build (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

    INSERT INTO rf_witness_probe SELECT number FROM numbers(20000);
    INSERT INTO rf_witness_build SELECT number FROM numbers(20000);
"

# Keep rf_witness_build on the build side: 'auto' may swap the sides and build the filter from
# the probe table. Fabricated join-order statistics can land at or below the threshold, in which
# case no runtime filter is created and find() is never called.
JOIN_SETTINGS="
    SET enable_analyzer = 1;
    SET enable_join_runtime_filters = 1;
    SET enable_parallel_replicas = 0;
    SET join_algorithm = 'hash';
    SET query_plan_join_swap_table = 0;
    SET query_plan_optimize_join_order_randomize = 0;
    SET join_runtime_filter_min_probe_rows = 0;
"

# The row counts are asserted, not discarded: an unfinished lookup that rejected rows instead of
# passing them through would still increment the witness below, but would lose matches here.
# Negative control: the ordinary finished path must not increment the witness.
$CLICKHOUSE_CLIENT -q "
    $JOIN_SETTINGS
    SELECT 'finished_rows', count() FROM rf_witness_probe AS l JOIN rf_witness_build AS r ON l.k = r.k
    SETTINGS log_comment = '04681_finished';
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT runtime_filter_skip_finish_insert"
$CLICKHOUSE_CLIENT -q "
    $JOIN_SETTINGS
    SELECT 'unfinished_rows', count() FROM rf_witness_probe AS l JOIN rf_witness_build AS r ON l.k = r.k
    SETTINGS log_comment = '04681_unfinished';
"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT runtime_filter_skip_finish_insert"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Three separate counters, never summed: the lookup path ran at all, the filter was created,
# and the unfinished branch was taken. The last one is only meaningful when the first two are
# positive.
# Each client invocation is its own session, so the read of system.query_log needs its own
# enable_parallel_replicas = 0 rather than inheriting it from the queries above.
$CLICKHOUSE_CLIENT -q "
    SET enable_parallel_replicas = 0;

    SELECT
        'unfinished',
        ProfileEvents['RuntimeFilterRowsChecked'] > 0 OR ProfileEvents['RuntimeFilterLookupsBeforeBuildFinished'] > 0 AS reached,
        ProfileEvents['RuntimeFiltersCreated'] > 0 AS created,
        ProfileEvents['RuntimeFilterLookupsBeforeBuildFinished'] > 0 AS unfinished_branch_taken
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04681_unfinished' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1;

    SELECT
        'finished',
        ProfileEvents['RuntimeFiltersCreated'] > 0 AS created,
        ProfileEvents['RuntimeFilterLookupsBeforeBuildFinished'] AS unfinished_branch_taken
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04681_finished' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rf_witness_probe;
    DROP TABLE rf_witness_build;
"
