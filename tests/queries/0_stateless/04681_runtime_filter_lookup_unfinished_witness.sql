-- Tags: no-parallel, no-fasttest
-- no-parallel: the fail point is process-global.
-- no-fasttest: fail points need a build with libfiu.

-- The fail point holds a registered runtime filter in the state it otherwise only passes
-- through transiently: findable by the probe side while inserts_are_finished is still false.
-- ProfileEvents make the branch taken inside IRuntimeFilter::find() observable, so the test
-- fails on a binary that does not carry the guard instead of depending on winning a race.

DROP TABLE IF EXISTS rf_witness_probe;
DROP TABLE IF EXISTS rf_witness_build;

CREATE TABLE rf_witness_probe (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE rf_witness_build (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

INSERT INTO rf_witness_probe SELECT number FROM numbers(20000);
INSERT INTO rf_witness_build SELECT number FROM numbers(20000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
-- Keep rf_witness_build on the build side; 'auto' may swap the sides and build the filter
-- from the probe table.
SET query_plan_join_swap_table = 0;
-- Fabricated join-order statistics can land at or below the threshold, in which case no
-- runtime filter is created and find() is never called.
SET query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_min_probe_rows = 0;

-- Negative control: the ordinary finished path must not increment the witness.
SELECT count() FROM rf_witness_probe AS l JOIN rf_witness_build AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '04681_finished';

SYSTEM ENABLE FAILPOINT runtime_filter_skip_finish_insert;
SELECT count() FROM rf_witness_probe AS l JOIN rf_witness_build AS r ON l.k = r.k
FORMAT Null SETTINGS log_comment = '04681_unfinished';
SYSTEM DISABLE FAILPOINT runtime_filter_skip_finish_insert;

SYSTEM FLUSH LOGS query_log;

-- Three separate counters, never summed: the lookup path ran at all, the filter was created,
-- and the unfinished branch was taken. The last one is only meaningful when the first two are
-- positive.
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

DROP TABLE rf_witness_probe;
DROP TABLE rf_witness_build;
