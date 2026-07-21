-- Regression test: `tryPushDownLimit` must not set the `DistinctStep` limit hint when a stateful
-- expression (e.g. `neighbor`, `logTrace`) sits below the distinct. The distinct transforms stop
-- reading input once the hint is reached, which would truncate the rows (and blocks) the stateful
-- expression observes. See https://github.com/ClickHouse/ClickHouse/pull/110188.

SET allow_deprecated_error_prone_window_functions = 1;
SET max_threads = 1;
SET max_block_size = 65536;

-- `neighbor(number, 1)` over the single block [0, 1, 2] is [1, 2, 0]; DISTINCT + LIMIT 1 keeps 1.
SELECT DISTINCT neighbor(number, 1) FROM numbers(3) LIMIT 1 SETTINGS enable_analyzer = 0;
SELECT DISTINCT neighbor(number, 1) FROM numbers(3) LIMIT 1 SETTINGS enable_analyzer = 1;

-- The same with the stateful expression below the distinct across a subquery boundary:
-- distinct sees [1, 2, 0] and emits the first two distinct values.
SELECT DISTINCT n FROM (SELECT neighbor(number, 1) AS n FROM numbers(3)) LIMIT 2 SETTINGS enable_analyzer = 0;
SELECT DISTINCT n FROM (SELECT neighbor(number, 1) AS n FROM numbers(3)) LIMIT 2 SETTINGS enable_analyzer = 1;
