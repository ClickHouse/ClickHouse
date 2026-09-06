-- Tags: no-fasttest

DROP TABLE IF EXISTS 04709_dst;
DROP TABLE IF EXISTS 04709_buf;

CREATE TABLE 04709_dst (k UInt8, arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst VALUES (1, '[1,2,3]');
CREATE TABLE 04709_buf (k UInt8, arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);
INSERT INTO 04709_buf VALUES (2, [9,9,9,9]);

SELECT 'rows', count() FROM (SELECT arr.size0 FROM 04709_buf);
SELECT 'explicit', k, arr, arr.size0, length(arr) FROM 04709_buf ORDER BY k
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'folded', k, arr, arr.size0 FROM 04709_buf ORDER BY k
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'aggregates', sum(length(arr)), sum(empty(arr)) FROM 04709_buf;

SELECT 'prewhere_ordinary', arr, arr.size0 FROM 04709_buf PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;

-- A filter consuming the parent while nothing else keeps it alive: the destination does not emit
-- the parent, so a derivation reading it back must not pick up a fabricated default.
SELECT 'prewhere_consumes_parent', k, arr.size0 FROM 04709_buf PREWHERE has(arr, 2) ORDER BY k;
SELECT 'prewhere_consumes_parent_element', k, arr.size0 FROM 04709_buf PREWHERE arr[1] = 1 ORDER BY k;
SELECT 'prewhere_consumes_parent_old_analyzer', k, arr.size0 FROM 04709_buf PREWHERE has(arr, 2) ORDER BY k
    SETTINGS enable_analyzer = 0;
SELECT 'prewhere_consumes_parent_selected', k, arr, arr.size0 FROM 04709_buf PREWHERE has(arr, 2) ORDER BY k;
SELECT 'prewhere_consumes_parent_other_expression', k, arr.size0, length(arr) FROM 04709_buf
    PREWHERE has(arr, 2) ORDER BY k SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'where_consumes_parent', k, arr.size0 FROM 04709_buf WHERE has(arr, 2) ORDER BY k;
SELECT 'where_on_subcolumn', k, arr.size0 FROM 04709_buf WHERE arr.size0 = 3 ORDER BY k;

SELECT 'row_policy_parent_effective', count() FROM 04709_buf;
CREATE ROW POLICY 04709_pol_parent ON 04709_buf USING has(arr, 2) TO ALL;
SELECT 'row_policy_parent_effective', count() FROM 04709_buf;
SELECT 'row_policy_parent', arr.size0 FROM 04709_buf;
DROP ROW POLICY 04709_pol_parent ON 04709_buf;

SELECT 'row_policy_effective', count() FROM 04709_buf;
CREATE ROW POLICY 04709_pol ON 04709_buf USING k = 1 TO ALL;
SELECT 'row_policy_effective', count() FROM 04709_buf;
SELECT 'row_policy', k, arr, arr.size0 FROM 04709_buf ORDER BY k
    SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY 04709_pol ON 04709_buf;

-- A row policy and a PREWHERE forward two filters, which run as consecutive steps over one block.
-- The parent must reach this table's type exactly once, no matter which step keeps it alive.
CREATE ROW POLICY 04709_pol_parent ON 04709_buf USING has(arr, 2) TO ALL;
SELECT 'two_filters_parent_policy_effective', count() FROM 04709_buf;
SELECT 'two_filters_parent_policy_selected', k, arr, arr.size0 FROM 04709_buf PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'two_filters_parent_policy_subcolumn', arr.size0 FROM 04709_buf PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'two_filters_parent_policy_no_prewhere', arr.size0 FROM 04709_buf
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'two_filters_both_consume_parent', k, arr.size0 FROM 04709_buf PREWHERE has(arr, 2)
    SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY 04709_pol_parent ON 04709_buf;

CREATE ROW POLICY 04709_pol ON 04709_buf USING k = 1 TO ALL;
SELECT 'two_filters_ordinary_policy_effective', count() FROM 04709_buf;
SELECT 'two_filters_ordinary_policy', k, arr.size0 FROM 04709_buf PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'two_filters_ordinary_policy_selected', k, arr, arr.size0 FROM 04709_buf PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'two_filters_prewhere_consumes_parent', k, arr.size0 FROM 04709_buf PREWHERE has(arr, 2)
    SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY 04709_pol ON 04709_buf;

DROP TABLE IF EXISTS 04709_dst_null;
DROP TABLE IF EXISTS 04709_buf_null;
CREATE TABLE 04709_dst_null (c String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_null VALUES ('x');
CREATE TABLE 04709_buf_null (c Nullable(String))
    ENGINE = Buffer(currentDatabase(), 04709_dst_null, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'nullable_null', count(), any(c), any(c.null) FROM 04709_buf_null;
-- The correct .null for a non-NULL row is the same 0 a type default supplies, so the row count is
-- the only assertion that can distinguish the two behaviours for this carrier.
SELECT 'nullable_null_rows', count() FROM (SELECT c.null FROM 04709_buf_null);

DROP TABLE IF EXISTS 04709_dst_tuple;
DROP TABLE IF EXISTS 04709_buf_tuple;
CREATE TABLE 04709_dst_tuple (t String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_tuple VALUES ('(1,2)');
CREATE TABLE 04709_buf_tuple (t Tuple(a UInt8, b UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dst_tuple, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'tuple_element', count(), any(t), any(t.a), any(t.b) FROM 04709_buf_tuple;

DROP TABLE IF EXISTS 04709_dst_nested;
DROP TABLE IF EXISTS 04709_buf_nested;
CREATE TABLE 04709_dst_nested (a String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_nested VALUES ('[[1,2],[3]]');
CREATE TABLE 04709_buf_nested (a Array(Array(UInt8)))
    ENGINE = Buffer(currentDatabase(), 04709_dst_nested, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'nested_array', count(), any(a), any(a.size0) FROM 04709_buf_nested;

DROP TABLE IF EXISTS 04709_dst_map;
DROP TABLE IF EXISTS 04709_buf_map;
-- A Map destination would resolve .keys/.values itself, so the parent type here must be one that
-- casts to Map but exposes no such subcolumn.
CREATE TABLE 04709_dst_map (m Array(Tuple(String, UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_map VALUES ([('k', 7)]);
CREATE TABLE 04709_buf_map (m Map(String, UInt64))
    ENGINE = Buffer(currentDatabase(), 04709_dst_map, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'map', count(), any(m), any(m.keys), any(m.values) FROM 04709_buf_map;
-- Two subcolumns of one parent behind a parent-consuming filter: the parent must be kept alive
-- exactly once, so assert the whole column set rather than a count.
SELECT 'map_prewhere_consumes_parent', m.keys, m.values FROM 04709_buf_map
    PREWHERE mapContains(m, 'k') SETTINGS optimize_functions_to_subcolumns = 0;
CREATE ROW POLICY 04709_pol_map ON 04709_buf_map USING mapContains(m, 'k') TO ALL;
SELECT 'map_two_filters_effective', count() FROM 04709_buf_map;
SELECT 'map_two_filters', m.keys, m.values FROM 04709_buf_map PREWHERE mapContains(m, 'k')
    SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY 04709_pol_map ON 04709_buf_map;

DROP TABLE IF EXISTS 04709_dst_lc;
DROP TABLE IF EXISTS 04709_buf_lc;
CREATE TABLE 04709_dst_lc (a String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_lc VALUES ('[\'x\',\'y\']');
CREATE TABLE 04709_buf_lc (a Array(LowCardinality(String)))
    ENGINE = Buffer(currentDatabase(), 04709_dst_lc, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'lowcardinality', count(), any(a), any(a.size0) FROM 04709_buf_lc;

DROP TABLE IF EXISTS 04709_dst_dyn;
DROP TABLE IF EXISTS 04709_buf_dyn;
CREATE TABLE 04709_dst_dyn (j JSON, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_dyn VALUES ('{"x":5}', [1,2,3]);
CREATE TABLE 04709_buf_dyn (j JSON, arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dst_dyn, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'dynamic_prewhere', j.x, arr.size0 FROM 04709_buf_dyn PREWHERE j.x = 5
    SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS 04709_dst_missing;
DROP TABLE IF EXISTS 04709_buf_missing;
CREATE TABLE 04709_dst_missing (k UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_missing VALUES (1);
CREATE TABLE 04709_buf_missing (k UInt8, arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dst_missing, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'missing_column', count(), any(arr), any(arr.size0) FROM 04709_buf_missing;
SELECT 'missing_column_rows', count() FROM (SELECT arr.size0 FROM 04709_buf_missing);

DROP TABLE IF EXISTS 04709_dst_same;
DROP TABLE IF EXISTS 04709_buf_same;
CREATE TABLE 04709_dst_same (k UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_same VALUES (1, [1,2,3]);
CREATE TABLE 04709_buf_same (k UInt8, arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dst_same, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'same_structure', count(), any(arr), any(arr.size0) FROM 04709_buf_same;
SELECT 'same_structure_prewhere', arr.size0 FROM 04709_buf_same PREWHERE k = 1;
-- Matched types make the conversion an identity, so two forwarded filters must be a no-op here.
CREATE ROW POLICY 04709_pol_same ON 04709_buf_same USING has(arr, 2) TO ALL;
SELECT 'same_structure_two_filters_effective', count() FROM 04709_buf_same;
SELECT 'same_structure_two_filters', k, arr, arr.size0 FROM 04709_buf_same PREWHERE k = 1
    SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'same_structure_two_filters_consume', k, arr, arr.size0 FROM 04709_buf_same
    PREWHERE has(arr, 2) SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY 04709_pol_same ON 04709_buf_same;

DROP TABLE IF EXISTS 04709_dst_ordinary;
DROP TABLE IF EXISTS 04709_buf_ordinary;
CREATE TABLE 04709_dst_ordinary (k UInt8, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_ordinary VALUES (1, '42');
CREATE TABLE 04709_buf_ordinary (k UInt8, s UInt64)
    ENGINE = Buffer(currentDatabase(), 04709_dst_ordinary, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT 'ordinary_mismatch', k, s FROM 04709_buf_ordinary PREWHERE k = 1;
-- No subcolumn is requested anywhere here, so the derivation code must stay inert even with two
-- forwarded filters over a mistyped column.
CREATE ROW POLICY 04709_pol_ordinary ON 04709_buf_ordinary USING k = 1 TO ALL;
SELECT 'ordinary_mismatch_two_filters_effective', count() FROM 04709_buf_ordinary;
SELECT 'ordinary_mismatch_two_filters', k, s FROM 04709_buf_ordinary PREWHERE k = 1;
DROP ROW POLICY 04709_pol_ordinary ON 04709_buf_ordinary;

DROP TABLE IF EXISTS 04709_dst_dist;
DROP TABLE IF EXISTS 04709_dist;
DROP TABLE IF EXISTS 04709_buf_dist;
CREATE TABLE 04709_dst_dist (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_dist VALUES ('[1,2,3,4,5,6]');
CREATE TABLE 04709_dist AS 04709_dst_dist
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04709_dst_dist);
CREATE TABLE 04709_buf_dist (arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dist, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT arr.size0 FROM 04709_buf_dist SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError NOT_IMPLEMENTED }
SELECT arr FROM 04709_buf_dist;

DROP TABLE IF EXISTS 04709_dst_dist_same;
DROP TABLE IF EXISTS 04709_dist_same;
DROP TABLE IF EXISTS 04709_buf_dist_same;
CREATE TABLE 04709_dst_dist_same (arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO 04709_dst_dist_same VALUES ([1,2,3]);
CREATE TABLE 04709_dist_same AS 04709_dst_dist_same
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04709_dst_dist_same);
CREATE TABLE 04709_buf_dist_same (arr Array(UInt8))
    ENGINE = Buffer(currentDatabase(), 04709_dist_same, 1, 100, 100, 1000, 10000, 1000000, 10000000);
SELECT arr, arr.size0 FROM 04709_buf_dist_same
    SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE 04709_buf;
DROP TABLE 04709_dst;
DROP TABLE 04709_buf_null;
DROP TABLE 04709_dst_null;
DROP TABLE 04709_buf_tuple;
DROP TABLE 04709_dst_tuple;
DROP TABLE 04709_buf_nested;
DROP TABLE 04709_dst_nested;
DROP TABLE 04709_buf_map;
DROP TABLE 04709_dst_map;
DROP TABLE 04709_buf_lc;
DROP TABLE 04709_dst_lc;
DROP TABLE 04709_buf_dyn;
DROP TABLE 04709_dst_dyn;
DROP TABLE 04709_buf_missing;
DROP TABLE 04709_dst_missing;
DROP TABLE 04709_buf_same;
DROP TABLE 04709_dst_same;
DROP TABLE 04709_buf_ordinary;
DROP TABLE 04709_dst_ordinary;
DROP TABLE 04709_buf_dist;
DROP TABLE 04709_dist;
DROP TABLE 04709_dst_dist;
DROP TABLE 04709_buf_dist_same;
DROP TABLE 04709_dist_same;
DROP TABLE 04709_dst_dist_same;
