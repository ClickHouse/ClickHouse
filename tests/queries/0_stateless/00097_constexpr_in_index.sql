-- Tags: stateful
-- Even in presence of OR, we evaluate the "0 IN (1, 2, 3)" as a constant expression therefore it does not prevent the index analysis.

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

SELECT count() FROM test.hits WHERE CounterID IN (14917930, 33034174) OR 0 IN (1, 2, 3) SETTINGS max_rows_to_read = 1000000, force_primary_key = 1;
