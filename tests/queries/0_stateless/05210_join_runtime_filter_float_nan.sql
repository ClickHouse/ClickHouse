-- A join runtime filter is a pre-filter, so it may only reject rows the join itself would not match.
-- The join's hash table compares keys bitwise, while `equals` reports NaN unequal to itself and 0.0
-- equal to -0.0, so a key that can carry a float must not take the single-element `equals` shortcut.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 1;
SET allow_dynamic_type_in_join_keys = 1;

-- 1. The reported symptom: a correlated scalar subquery over an all-NaN column is decorrelated into a
-- join, and the runtime filter emptied its probe side, so every scalar came back NULL.
CREATE TABLE nan_scalar (id UInt64, f Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO nan_scalar SELECT number, nan FROM numbers(3);

SELECT toString((SELECT f)) FROM nan_scalar ORDER BY id;
SELECT count() FROM (SELECT id FROM nan_scalar WHERE ((SELECT f) IS NULL) OR (f < -1));

-- The remaining scenarios compare the two `enable_join_runtime_filters` arms instead of a literal
-- count, so they keep asserting the invariant if the join itself ever changes what it matches.

-- 2. A plain INNER JOIN on a NaN key: the filter must not reject the row the join does match.
CREATE TABLE nan_key (f Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nan_key VALUES (nan);

SELECT 'inner join, NaN Float64 key',
       (SELECT count() FROM nan_key a JOIN nan_key b ON a.f = b.f SETTINGS enable_join_runtime_filters = 1)
     = (SELECT count() FROM nan_key a JOIN nan_key b ON a.f = b.f SETTINGS enable_join_runtime_filters = 0) AS arms_agree;

-- 3. ANTI JOIN, where the filter excludes instead of selecting, and the value is -0.0 rather than NaN.
-- The float sits inside an Array, so the key type has to be inspected recursively.
CREATE TABLE pos_zero (a Array(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pos_zero VALUES ([0.0]);
CREATE TABLE neg_zero (a Array(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO neg_zero VALUES ([-0.0]);

SELECT 'anti join, -0.0 inside an Array(Float64) key',
       (SELECT count() FROM pos_zero a ANTI LEFT JOIN neg_zero b ON a.a = b.a SETTINGS enable_join_runtime_filters = 1)
     = (SELECT count() FROM pos_zero a ANTI LEFT JOIN neg_zero b ON a.a = b.a SETTINGS enable_join_runtime_filters = 0) AS arms_agree;

-- 4. A JSON key. Its float paths are discovered while reading, so the whole type has to be rejected.
CREATE TABLE json_pos_zero (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_pos_zero VALUES ('{"x":0.0}');
CREATE TABLE json_neg_zero (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO json_neg_zero VALUES ('{"x":-0.0}');

SELECT 'anti join, -0.0 on a JSON path key',
       (SELECT count() FROM json_pos_zero a ANTI LEFT JOIN json_neg_zero b ON a.j = b.j SETTINGS enable_join_runtime_filters = 1)
     = (SELECT count() FROM json_pos_zero a ANTI LEFT JOIN json_neg_zero b ON a.j = b.j SETTINGS enable_join_runtime_filters = 0) AS arms_agree;

-- Liveness. The comparisons above would also pass with no runtime filter installed at all, because
-- FunctionApplyFilter passes every row when the filter is absent, and the Int64 control below only
-- exercises the `equals` shortcut that a float key no longer takes. Assert each filter really ran.
SELECT count() FROM nan_key a JOIN nan_key b ON a.f = b.f
SETTINGS log_comment = '05210_live_f64', max_threads = 1;

SELECT count() FROM pos_zero a ANTI LEFT JOIN neg_zero b ON a.a = b.a
SETTINGS log_comment = '05210_live_array', max_threads = 1;

SELECT count() FROM json_pos_zero a ANTI LEFT JOIN json_neg_zero b ON a.j = b.j
SETTINGS log_comment = '05210_live_json', max_threads = 1;

-- 5. Control on an Int64 key, where `equals` does agree with the hash table. It keeps the shortcut,
-- and the profile events show the filter is still built and still rejects rows.
CREATE TABLE int_build (k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO int_build VALUES (7);
CREATE TABLE int_probe (k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO int_probe SELECT number FROM numbers(1000);

SELECT count() FROM int_probe a JOIN int_build b ON a.k = b.k
SETTINGS log_comment = '05210_int_control', max_threads = 1;

SYSTEM FLUSH LOGS query_log;

SELECT 'runtime filter ran on every float-bearing key',
       uniqExactIf(log_comment, ProfileEvents['RuntimeFilterRowsChecked'] > 0) = 3 AS all_engaged
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
  AND log_comment IN ('05210_live_f64', '05210_live_array', '05210_live_json')
  AND event_date >= yesterday();

SELECT 'control: Int64 key, filter built and rejecting rows',
       ProfileEvents['RuntimeFilterRowsChecked'] > 0
   AND ProfileEvents['RuntimeFilterRowsPassed'] < ProfileEvents['RuntimeFilterRowsChecked'] AS filter_pruned
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = '05210_int_control'
  AND current_database = currentDatabase() AND event_date >= yesterday();
