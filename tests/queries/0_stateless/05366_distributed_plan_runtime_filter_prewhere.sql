-- A join runtime filter moved into `PREWHERE` keeps filtering in a shipped plan fragment, and the
-- fragment's hash join still publishes the shared `FixedHashMap` filter.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
-- The assertions below count the rows the filter in PREWHERE lets through.
SET optimize_move_to_prewhere = 1;
SET join_use_nulls = 0;
-- The shared `FixedHashMap` filter is published only by a non-spilling `hash` join.
SET join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_runtime_filter_from_fixed_hash_table = 1;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build;

CREATE TABLE probe (k UInt64, v String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;
CREATE TABLE build (k UInt64, c UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;

INSERT INTO probe SELECT number, toString(number) FROM numbers(200000);
-- Dense keys, so the hash join converts its map into a `FixedHashMap` and publishes the shared filter.
INSERT INTO build SELECT number * 2, number % 2 FROM numbers(200);

-- One fragment, so the join and both reads are shipped together.
SELECT count(), sum(length(v)) FROM probe JOIN build ON probe.k = build.k WHERE build.c = 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
    param__internal_cascades_cost_config = '{"exchange_fixed_overhead":1000000000}', query_plan_join_swap_table = 0,
    log_comment = '05366_fragment';

-- The small table on the left: Cascades swaps the join inputs after the filter was added, so the
-- fragment makes the misplaced filter inert and adds one that filters `probe`.
SELECT count(), sum(length(v)) FROM build JOIN probe ON probe.k = build.k WHERE build.c = 1
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_cascades_optimizer = 1, distributed_plan_workers_num = 2,
    param__internal_cascades_cost_config = '{"exchange_fixed_overhead":1000000000}', query_plan_join_swap_table = 0,
    log_comment = '05366_fragment_swapped';

SELECT count(), sum(length(v)) FROM probe JOIN build ON probe.k = build.k WHERE build.c = 1
SETTINGS query_plan_join_swap_table = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log, text_log;

-- Each of the two tasks creates one filter and publishes the shared one, and the reads of `probe` output
-- the 100 matching of its 200000 rows plus a few bloom filter false positives.
SELECT log_comment, sum(filters_created), sum(published), countIf(reads.query_id != ''), sum(probe_rows) BETWEEN 100 AND 1000
FROM
(
    SELECT query_id, log_comment, ProfileEvents['RuntimeFiltersCreated'] AS filters_created
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id IN (SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
                               WHERE event_date >= yesterday() AND current_database = currentDatabase()
                                 AND log_comment IN ('05366_fragment', '05366_fragment_swapped') AND type = 'QueryFinish' AND is_initial_query
                               GROUP BY log_comment)
) AS tasks
LEFT JOIN
(
    SELECT query_id, count() AS published
    FROM system.text_log
    WHERE event_date >= yesterday() AND message LIKE 'Published shared fixed-hash-table runtime filter%'
    GROUP BY query_id
) AS publications USING query_id
LEFT JOIN
(
    SELECT query_id, sum(output_rows) AS probe_rows
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND name LIKE 'MergeTreeSelect%' AND plan_step_description = currentDatabase() || '.probe'
    GROUP BY query_id
) AS reads ON reads.query_id = tasks.query_id
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE probe;
DROP TABLE build;
