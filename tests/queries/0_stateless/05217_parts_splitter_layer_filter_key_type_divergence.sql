-- The layer filter of `PartsSplitter` used to re-resolve the sorting key AST in the query context. A key
-- expression whose result type depends on a setting - `CAST(json.b, 'String')` is `String` in the table,
-- created under `cast_keep_nullable = 0`, and `Nullable(String)` in a session with `cast_keep_nullable = 1` -
-- then became a different function with different values, the filter compared the query-side value with
-- the borders the table-side value was split by, and the row with a `NULL` key fell out of every layer.
-- The filter now refers to the key columns the table's own sorting expression has already computed.

SET cast_keep_nullable = 0;

DROP TABLE IF EXISTS t_final;
DROP TABLE IF EXISTS t_split;

CREATE TABLE t_final (json JSON) ENGINE = ReplacingMergeTree ORDER BY CAST(json.b, 'String') SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_final;
INSERT INTO t_final VALUES ('{"b":"a"}'), ('{"b":"c"}');
INSERT INTO t_final VALUES ('{"b":"b"}'), ('{"a":1}');
INSERT INTO t_final VALUES ('{"b":"b"}'), ('{"b":"d"}');

CREATE TABLE t_split (json JSON) ENGINE = MergeTree ORDER BY CAST(json.b, 'String') SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_split;
INSERT INTO t_split VALUES ('{"b":"a"}'), ('{"b":"c"}');
INSERT INTO t_split VALUES ('{"b":"b"}'), ('{"a":1}');

SET cast_keep_nullable = 1;

-- `FINAL` splits the intersecting parts into primary key range layers, one per thread.
SELECT 'FINAL, diverged key type';
SELECT CAST(json.b, 'String') AS v FROM t_final FINAL ORDER BY v
SETTINGS max_threads = 4, do_not_merge_across_partitions_select_final = 0,
         split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;

-- The same split, forced on an ordinary read.
SELECT 'ordinary read, forced split, diverged key type';
SELECT CAST(json.b, 'String') AS v FROM t_split ORDER BY v
SETTINGS max_threads = 4, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1;
SELECT count(), countIf(json.b IS NULL) FROM t_split
SETTINGS max_threads = 4, optimize_trivial_count_query = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1;

-- Control: with the types in agreement the result is the same.
SELECT 'FINAL, same key type';
SELECT CAST(json.b, 'String') AS v FROM t_final FINAL ORDER BY v
SETTINGS cast_keep_nullable = 0, max_threads = 4, do_not_merge_across_partitions_select_final = 0,
         split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;

DROP TABLE t_final;
DROP TABLE t_split;
