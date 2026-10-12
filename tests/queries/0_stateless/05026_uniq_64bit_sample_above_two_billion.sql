-- Tags: long, no-fasttest, no-tsan, no-asan, no-msan, no-ubsan
-- Tag no-fasttest, no-*san: aggregates four billion rows, which is too slow there.

-- Above about two billion elements the estimate of `uniq` switches to the sample
-- of 64-bit hashes carried by version 1 of the state (issue #6078: the 32-bit estimate
-- used to saturate and then overflow into garbage like 18446743978444128518).
-- The result does not depend on the distribution of the rows between the threads,
-- but `max_threads` is pinned because a randomized single-thread run does not finish in time.
SELECT uniq(number) FROM numbers_mt(4000000000)
SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0,
    max_execution_time = 0, max_estimated_execution_time = 0, max_threads = 16;
