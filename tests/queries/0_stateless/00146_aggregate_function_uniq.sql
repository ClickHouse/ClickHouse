-- Tags: stateful, no-msan
-- no-msan: `uniqExact(WatchID) FROM test.hits` materializes every distinct WatchID into a
-- HashSet over ~100M rows; under MSan the per-access shadow check cost combined with adversarial
-- random settings (small `max_block_size`, `max_threads = 2`, `compile_aggregate_expressions = 0`,
-- `merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability` close
-- to 1) pushes the test past the 600s per-test timeout. MSan coverage of `uniq`/`uniqHLL12`/
-- `uniqCombined`/`uniqExact` is provided by the smaller stateless tests (e.g. `00264_uniq_many_args`).

SELECT RegionID, uniqHLL12(WatchID) AS X FROM remote('127.0.0.{1,2}', test, hits) GROUP BY RegionID HAVING X > 100000 ORDER BY RegionID ASC;
SELECT RegionID, uniqCombined(WatchID) AS X FROM remote('127.0.0.{1,2}', test, hits) GROUP BY RegionID HAVING X > 100000 ORDER BY RegionID ASC;
SELECT abs(uniq(WatchID) - uniqExact(WatchID)) FROM test.hits;
