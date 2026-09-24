-- JOIN runtime-filter granule pruning must stay off for a FINAL read: a part's skip index
-- describes the rows stored in it, not the rows that survive the merge, so pruning by the
-- probe key would let a superseded row win.

DROP TABLE IF EXISTS final_fact;
DROP TABLE IF EXISTS final_dim;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is disabled under parallel replicas, so pin it off to reach the guard.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS final_fact;
DROP TABLE IF EXISTS final_dim;

CREATE TABLE final_fact (id UInt64, ver UInt64, k UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(ver) ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE final_dim (k UInt64) ENGINE = MergeTree ORDER BY k;

-- Every version must stay in its own part: once merged, each granule holds the surviving row
-- and the pruning decision no longer changes the answer.
SYSTEM STOP MERGES final_fact;

-- id < 1000: the superseded row matches the dim key, the surviving row does not.
INSERT INTO final_fact SELECT number, 1, 7 FROM numbers(1000);
INSERT INTO final_fact SELECT number, 2, 99999 FROM numbers(1000);
-- id >= 1000: the other way round.
INSERT INTO final_fact SELECT number + 1000, 1, 99999 FROM numbers(1000);
INSERT INTO final_fact SELECT number + 1000, 2, 7 FROM numbers(1000);
INSERT INTO final_dim VALUES (7);

SELECT count() FROM final_fact AS f FINAL INNER JOIN final_dim AS d ON f.k = d.k;
SELECT count() FROM final_fact AS f INNER JOIN final_dim AS d ON f.k = d.k;

DROP TABLE final_fact;
DROP TABLE final_dim;
