-- Tags: no-parallel-replicas
-- The EXPLAIN PLAN assertions below depend on the count()-over-ARRAY-JOIN rewrite, which is an
-- analyzer pass; the plan shape differs under parallel replicas.

-- Tests that count() over an ARRAY JOIN, whose element values are never referenced, is rewritten to
-- sum() over the array lengths so only the lightweight arr.size0 subcolumn is read instead of the
-- whole array. See issue #110812.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_parallel_replicas = 0;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projections = 0;
-- The rewrite declines under unaligned array join (row count is the max length, not one array's
-- length).
SET enable_unaligned_array_join = 0;

DROP TABLE IF EXISTS t_count_aj;
CREATE TABLE t_count_aj (id UInt64, arr Array(UInt64), narr Array(Nullable(String)), lcarr Array(LowCardinality(String)), m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;

-- Include empty arrays to exercise LEFT ARRAY JOIN semantics.
INSERT INTO t_count_aj
SELECT number, if(number % 7 = 0, [], range(number % 10)),
       arrayMap(x -> if(x % 2 = 0, NULL, toString(x)), range(number % 5)),
       arrayMap(x -> toString(x % 3), range(number % 4)),
       (SELECT map('k1', number, 'k2', number + 1))
FROM numbers(1000);

SELECT 'Correctness: count() must be unchanged and equal to sum(length(arr)) / sum(greatest(length(arr), 1)) for LEFT.';
SELECT count() FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(if(empty(arr), 1, length(arr))) FROM t_count_aj) FROM t_count_aj LEFT ARRAY JOIN arr AS value;
SELECT count() = (SELECT sum(length(narr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN narr;
SELECT count() = (SELECT sum(length(lcarr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN lcarr;
SELECT count() = (SELECT sum(length(m)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN m;
-- count(*) and count(1) are also plain row counts.
SELECT count(*) = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count(1) = (SELECT sum(length(arr)) FROM t_count_aj) FROM t_count_aj ARRAY JOIN arr AS value;
-- count(NULL) always returns 0 (a NULL argument is never counted); the rewrite must NOT fire for it.
SELECT count(NULL) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count(NULL::Nullable(UInt64)) FROM t_count_aj ARRAY JOIN arr AS value;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count(NULL) FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';

-- The output column name must remain count() (the rewrite must not rename the projection).
DESCRIBE (SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) FORMAT TSVRaw;

SELECT 'Optimization: the plan must read arr.size0 and no longer contain an ARRAY JOIN step.';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj LEFT ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN narr) WHERE explain ILIKE '%narr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN lcarr) WHERE explain ILIKE '%lcarr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN m) WHERE explain ILIKE '%m.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%ARRAY JOIN%';
-- No placeholder array is materialized: the plan must aggregate with sum() and must not contain
-- arrayWithConstant. This is what makes the rewrite immune to the array-size cap that a materialized
-- placeholder array would hit for a large count() (issue #110812).
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%sum(%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arrayWithConstant%';

SELECT 'When the array is read elsewhere, the rewrite must NOT fire, so the plan keeps the ARRAY JOIN and never adds a synthetic array. Results must be unchanged.';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT sum(value) FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr) WHERE explain ILIKE '%arrayWithConstant%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT arr FROM t_count_aj ARRAY JOIN arr AS value) WHERE explain ILIKE '%ARRAY JOIN%';
-- GROUP BY over the exploded array (INNER) drops empty-array groups; the count must be unchanged.
SELECT count() FROM (SELECT arr, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY arr);

SELECT 'Join-tree shapes the rewrite does not handle must decline (keep the ARRAY JOIN) and stay equal to the optimization-off result.';
-- Chained ARRAY JOIN (cartesian product of the arrays): the ARRAY JOIN input is another ARRAY JOIN, not a table.
SELECT (SELECT count() FROM t_count_aj ARRAY JOIN arr ARRAY JOIN narr) = (SELECT count() FROM t_count_aj ARRAY JOIN arr ARRAY JOIN narr SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr ARRAY JOIN narr) WHERE explain ILIKE '%ARRAY JOIN%';
-- A regular JOIN in the same query: the join tree root is a JOIN, not an ARRAY JOIN over a table.
SELECT (SELECT count() FROM t_count_aj AS a INNER JOIN t_count_aj AS b ON a.id = b.id ARRAY JOIN a.arr) = (SELECT count() FROM t_count_aj AS a INNER JOIN t_count_aj AS b ON a.id = b.id ARRAY JOIN a.arr SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj AS a INNER JOIN t_count_aj AS b ON a.id = b.id ARRAY JOIN a.arr) WHERE explain ILIKE '%ARRAY JOIN%';
-- GROUP BY in the same query: the count is per group, not a single row count.
SELECT (SELECT count() FROM (SELECT id, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY id)) = (SELECT count() FROM (SELECT id, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY id) SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT id, count() FROM t_count_aj ARRAY JOIN arr AS value GROUP BY id) WHERE explain ILIKE '%ARRAY JOIN%';

-- With the setting disabled, the optimization must not fire (backward compatible). Assert the ARRAY
-- JOIN survives and no sum() replaced the count(), not merely that arr.size0 is absent: this pass and
-- the downstream subcolumn pass read the same setting, so a size0-only check would still pass if this
-- pass ignored it and only the subcolumn folding declined.
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj ARRAY JOIN arr AS value SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%sum(length%';

SELECT 'A non-LEFT ARRAY JOIN over only-empty arrays leaves the aggregation with no input, so empty_result_for_aggregation_by_empty_set must keep returning an empty result and the rewrite must decline.';
DROP TABLE IF EXISTS t_count_aj_empty;
CREATE TABLE t_count_aj_empty (id UInt64, arr Array(UInt64), m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_count_aj_empty SELECT number, [], map() FROM numbers(3);
-- Every array is empty, so INNER ARRAY JOIN emits no row at all: the result must be empty, not 0.
SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count(*) FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count() FROM t_count_aj_empty ARRAY JOIN m SETTINGS empty_result_for_aggregation_by_empty_set = 1;
-- Assert the ARRAY JOIN is still there, not just that arr.size0 is absent: a pass that ignored the
-- setting would drop the ARRAY JOIN for a plain sum(length(arr)) and still satisfy a size0-only check.
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) WHERE explain ILIKE '%sum(length%';
-- The setting is per-query-scope, so an inner SELECT that carries it must decline on its own.
SELECT * FROM (SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1);
-- LEFT ARRAY JOIN emits one row per input row, so its aggregation input is empty only when the table
-- is: the rewrite stays enabled and both paths agree.
SELECT count() FROM t_count_aj_empty LEFT ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_empty LEFT ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) WHERE explain ILIKE '%arr.size0%';
-- An empty table yields an empty aggregation input either way, so both keywords must return no row.
DROP TABLE IF EXISTS t_count_aj_norows;
CREATE TABLE t_count_aj_norows (id UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
SELECT count() FROM t_count_aj_norows ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count() FROM t_count_aj_norows LEFT ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1;
-- With the setting off, the non-LEFT rewrite must still fire and stay equal to the unoptimized path.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 0) WHERE explain ILIKE '%arr.size0%';
SELECT (SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 0) = (SELECT count() FROM t_count_aj_empty ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 0, optimize_functions_to_subcolumns = 0);
-- A table with some non-empty arrays keeps a non-empty aggregation input, so the setting changes
-- nothing there and the results must match the unoptimized path.
SELECT (SELECT count() FROM t_count_aj ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) = (SELECT count() FROM t_count_aj ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1, optimize_functions_to_subcolumns = 0);
SELECT (SELECT count() FROM t_count_aj LEFT ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1) = (SELECT count() FROM t_count_aj LEFT ARRAY JOIN arr SETTINGS empty_result_for_aggregation_by_empty_set = 1, optimize_functions_to_subcolumns = 0);
DROP TABLE t_count_aj_norows;
DROP TABLE t_count_aj_empty;

SELECT 'A storage that opts out of subcolumn optimization may read a different schema than the one analyzed here, so the rewrite must decline and leave the ARRAY JOIN to validate the type where the read happens.';
DROP TABLE IF EXISTS t_count_aj_shard;
DROP TABLE IF EXISTS t_count_aj_dist;
CREATE TABLE t_count_aj_shard (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_count_aj_shard SELECT range(number % 4) FROM numbers(10);
CREATE TABLE t_count_aj_dist (arr Array(UInt64)) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_count_aj_shard);
SELECT count() = (SELECT sum(length(arr)) FROM t_count_aj_shard) FROM t_count_aj_dist ARRAY JOIN arr;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_dist ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
-- Pin prefer_localhost_replica: with the remote path the initiator plan is a bare ReadFromRemote, so
-- the ARRAY JOIN step is only observable in the local-replica plan.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_dist ARRAY JOIN arr SETTINGS prefer_localhost_replica = 1) WHERE explain ILIKE '%ARRAY JOIN%';
-- Declining at the initiator costs nothing: the shard re-analyzes the untouched query and rewrites it
-- there, where the declared type is the one actually read.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_count_aj_shard ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
DROP TABLE t_count_aj_dist;
DROP TABLE t_count_aj_shard;

SELECT 'A Merge table declares its own column list, so a child may hold a different type for the same column name. supportsOptimizationToSubcolumns() is the AND over children and a MergeTree child satisfies it whatever its types are, so the rewrite must check the children.';
DROP TABLE IF EXISTS t_caj_mh_good;
DROP TABLE IF EXISTS t_caj_mh_bad;
DROP TABLE IF EXISTS t_caj_mh;
CREATE TABLE t_caj_mh_good (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
-- A String holding '[4,5,6]' has length 7 as a string and 3 as an array, so reading the child's own
-- type instead of the declared one gives a different count().
CREATE TABLE t_caj_mh_bad (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mh_good VALUES ([1, 2]), ([]), ([3]);
INSERT INTO t_caj_mh_bad VALUES ('[4,5,6]');
CREATE TABLE t_caj_mh (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mh_(good|bad)$');
SELECT count() FROM t_caj_mh ARRAY JOIN arr;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mh ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mh ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
-- Assert the ARRAY JOIN survives, not just that arr.size0 is absent: a pass that dropped the ARRAY
-- JOIN and let FunctionToSubcolumnsPass decline would still satisfy a size0-only check.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mh ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
-- LEFT ARRAY JOIN and a Map column reach the same guard, which sits above both the isLeft() branch
-- and any type-specific handling.
SELECT count() FROM t_caj_mh LEFT ARRAY JOIN arr;
DROP TABLE IF EXISTS t_caj_mm_good;
DROP TABLE IF EXISTS t_caj_mm_bad;
DROP TABLE IF EXISTS t_caj_mm;
CREATE TABLE t_caj_mm_good (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_caj_mm_bad (m Map(String, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mm_good VALUES (map('a', 1, 'b', 2)), (map());
INSERT INTO t_caj_mm_bad VALUES (map('c', '3', 'd', '4', 'e', '5'));
CREATE TABLE t_caj_mm (m Map(String, UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mm_(good|bad)$');
SELECT count() FROM t_caj_mm ARRAY JOIN m;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mm ARRAY JOIN m) WHERE explain ILIKE '%ARRAY JOIN%';

SELECT 'A Merge table whose children all declare the analyzed type is safe, so the rewrite must still fire there: keying on the engine instead of on the actual types would silently drop the optimization.';
DROP TABLE IF EXISTS t_caj_mo_a;
DROP TABLE IF EXISTS t_caj_mo_b;
DROP TABLE IF EXISTS t_caj_mo;
DROP TABLE IF EXISTS t_caj_mo_one;
CREATE TABLE t_caj_mo_a (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_caj_mo_b (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mo_a VALUES ([1, 2]), ([]);
INSERT INTO t_caj_mo_b VALUES ([3, 4, 5]);
CREATE TABLE t_caj_mo (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mo_(a|b)$');
SELECT count() FROM t_caj_mo ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mo ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mo ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
-- A single matched child is the degenerate case where inverting the per-child predicate would be
-- invisible in the two-child table above.
CREATE TABLE t_caj_mo_one (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mo_a$');
SELECT count() FROM t_caj_mo_one ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mo_one ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mo_one ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';

SELECT 'The per-child check inspects direct children only, so a Merge whose child is itself a Merge declines whether or not the grandchildren match: the inner level re-plans the already-transformed query and cannot restore the removed ARRAY JOIN.';
DROP TABLE IF EXISTS t_caj_mni_good;
DROP TABLE IF EXISTS t_caj_mni_bad;
DROP TABLE IF EXISTS t_caj_mni;
DROP TABLE IF EXISTS t_caj_mno;
CREATE TABLE t_caj_mni_good (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_caj_mni_bad (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mni_good VALUES ([1, 2]), ([3]);
INSERT INTO t_caj_mni_bad VALUES ('[7,8,9]');
CREATE TABLE t_caj_mni (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mni_(good|bad)$');
-- The outer regexp must match ONLY the inner Merge, otherwise the grandchildren become direct
-- children and this stops testing the nested case.
CREATE TABLE t_caj_mno (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mni$');
SELECT count() FROM t_caj_mno ARRAY JOIN arr;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mno ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mno ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
-- A nested Merge whose every leaf DOES match the analyzed type also declines. That costs the
-- optimization and is deliberate: descending would need re-entrancy and cycle handling, while
-- declining is monotone. This row pins the cost so adding recursion later flips exactly one row.
DROP TABLE IF EXISTS t_caj_mnh_a;
DROP TABLE IF EXISTS t_caj_mnh_b;
DROP TABLE IF EXISTS t_caj_mnh_i;
DROP TABLE IF EXISTS t_caj_mnh_o;
CREATE TABLE t_caj_mnh_a (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_caj_mnh_b (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mnh_a VALUES ([1, 2]);
INSERT INTO t_caj_mnh_b VALUES ([3, 4, 5]);
CREATE TABLE t_caj_mnh_i (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mnh_(a|b)$');
CREATE TABLE t_caj_mnh_o (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_mnh_i$');
SELECT count() FROM t_caj_mnh_o ARRAY JOIN arr;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mnh_o ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mnh_o ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';

SELECT 'Alias forwards both supportsOptimizationToSubcolumns() and the read, so the per-child check must see the storage that executes rather than the wrapper.';
DROP TABLE IF EXISTS t_caj_alias_bad;
DROP TABLE IF EXISTS t_caj_alias_good;
CREATE TABLE t_caj_alias_bad ENGINE = Alias(currentDatabase(), 't_caj_mh');
SELECT count() FROM t_caj_alias_bad ARRAY JOIN arr;
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_alias_bad ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_alias_bad ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
-- Resolving the wrapper must not turn into a blanket decline on every wrapper.
CREATE TABLE t_caj_alias_good ENGINE = Alias(currentDatabase(), 't_caj_mo');
SELECT count() FROM t_caj_alias_good ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_alias_good ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_alias_good ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
DROP TABLE t_caj_alias_good;
DROP TABLE t_caj_alias_bad;
DROP TABLE t_caj_mnh_o;
DROP TABLE t_caj_mnh_i;
DROP TABLE t_caj_mnh_b;
DROP TABLE t_caj_mnh_a;
DROP TABLE t_caj_mno;
DROP TABLE t_caj_mni;
DROP TABLE t_caj_mni_bad;
DROP TABLE t_caj_mni_good;
DROP TABLE t_caj_mo_one;
DROP TABLE t_caj_mo;
DROP TABLE t_caj_mo_b;
DROP TABLE t_caj_mo_a;
DROP TABLE t_caj_mm;
DROP TABLE t_caj_mm_bad;
DROP TABLE t_caj_mm_good;
DROP TABLE t_caj_mh;
DROP TABLE t_caj_mh_bad;
DROP TABLE t_caj_mh_good;

SELECT 'Buffer, View and MaterializedView satisfy the subcolumn-capability check by forwarding it to their destination, target or inner query, and none of them is a wrapper the resolution loop unwraps, so the read can execute against a schema this analysis never saw. The rewrite must decline on all three and stay equal to the optimization-off result.';
DROP TABLE IF EXISTS t_caj_bd_bad;
DROP TABLE IF EXISTS t_caj_buf_bad;
DROP TABLE IF EXISTS t_caj_bd_ok;
DROP TABLE IF EXISTS t_caj_buf_ok;
-- The mismatched Buffer below makes the server log "Destination table ... has different type of
-- column arr" per read, which the test runner treats as a failure. Scoped to this section, as
-- 00158_buffer_and_nonexistent_table.sql does for its own expected Buffer warning.
SET send_logs_level = 'fatal';
-- The destination declares arr String while the Buffer declares Array(UInt64). Reading the
-- destination's own type gives a different count(), and dropping the ARRAY JOIN drops the type check
-- that would have caught it.
CREATE TABLE t_caj_bd_bad (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_bd_bad VALUES ('[4,5,6]');
CREATE TABLE t_caj_buf_bad (arr Array(UInt64)) ENGINE = Buffer(currentDatabase(), 't_caj_bd_bad', 1, 1000, 1000, 10, 100, 10000, 1000000);
SELECT count() = (SELECT count() FROM t_caj_buf_bad ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_buf_bad ARRAY JOIN arr;
-- Assert the ARRAY JOIN survives, not merely that arr.size0 is absent: a pass that dropped the ARRAY
-- JOIN and let FunctionToSubcolumnsPass decline would still satisfy a size0-only check.
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_buf_bad ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_buf_bad ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_buf_bad ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
-- Must not regress: the decline costs the optimization but never the answer, so a Buffer whose
-- destination declares the analyzed type still counts correctly.
CREATE TABLE t_caj_bd_ok (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_bd_ok VALUES ([1, 2]), ([]), ([3]);
CREATE TABLE t_caj_buf_ok (arr Array(UInt64)) ENGINE = Buffer(currentDatabase(), 't_caj_bd_ok', 1, 1000, 1000, 10, 100, 10000, 1000000);
SELECT count() FROM t_caj_buf_ok ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_buf_ok ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_buf_ok ARRAY JOIN arr;

DROP TABLE IF EXISTS t_caj_vw_bad;
DROP TABLE IF EXISTS t_caj_vw_ok;
-- The view declares arr Array(UInt64) over an inner query returning String.
CREATE VIEW t_caj_vw_bad (arr Array(UInt64)) AS SELECT arr FROM t_caj_bd_bad;
SELECT count() = (SELECT count() FROM t_caj_vw_bad ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_vw_bad ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_vw_bad ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_vw_bad ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_vw_bad ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
CREATE VIEW t_caj_vw_ok AS SELECT arr FROM t_caj_bd_ok;
SELECT count() FROM t_caj_vw_ok ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_vw_ok ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_vw_ok ARRAY JOIN arr;

DROP TABLE IF EXISTS t_caj_mvt_bad;
DROP TABLE IF EXISTS t_caj_mv_bad;
DROP TABLE IF EXISTS t_caj_mvt_ok;
DROP TABLE IF EXISTS t_caj_mv_ok;
-- The declared column list follows TO <table>, and here it disagrees with the target.
CREATE TABLE t_caj_mvt_bad (arr String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mvt_bad VALUES ('[4,5,6]');
CREATE MATERIALIZED VIEW t_caj_mv_bad TO t_caj_mvt_bad (arr Array(UInt64)) AS SELECT arr FROM t_caj_bd_ok;
SELECT count() = (SELECT count() FROM t_caj_mv_bad ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_mv_bad ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mv_bad ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mv_bad ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mv_bad ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
CREATE TABLE t_caj_mvt_ok (arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_caj_mvt_ok VALUES ([1, 2]), ([]), ([3]);
CREATE MATERIALIZED VIEW t_caj_mv_ok TO t_caj_mvt_ok (arr Array(UInt64)) AS SELECT arr FROM t_caj_bd_ok;
SELECT count() FROM t_caj_mv_ok ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_mv_ok ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_mv_ok ARRAY JOIN arr;

SELECT 'The per-child check must decline the same three storages, otherwise a Buffer or MaterializedView child whose declared type matches is admitted and the Merge path inherits the identical exposure.';
DROP TABLE IF EXISTS t_caj_mbuf;
-- The child is a Buffer declaring the analyzed type, so only the engine-based decline stops it.
CREATE TABLE t_caj_mbuf (arr Array(UInt64)) ENGINE = Merge(currentDatabase(), '^t_caj_buf_ok$');
SELECT count() = (SELECT count() FROM t_caj_mbuf ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_mbuf ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mbuf ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_mbuf ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
DROP TABLE t_caj_mbuf;
DROP TABLE t_caj_mv_ok;
DROP TABLE t_caj_mvt_ok;
DROP TABLE t_caj_mv_bad;
DROP TABLE t_caj_mvt_bad;
DROP TABLE t_caj_vw_ok;
DROP TABLE t_caj_vw_bad;
DROP TABLE t_caj_buf_ok;
DROP TABLE t_caj_bd_ok;
DROP TABLE t_caj_buf_bad;
DROP TABLE t_caj_bd_bad;
-- The mismatched Buffer is gone, so restore the default log level for the rest of the file.
SET send_logs_level = 'warning';

SELECT 'The rewrite drops the ARRAY JOIN before knowing whether FunctionToSubcolumnsPass will fold length() into .size0, and that fold skips index columns and returns early under FINAL. The count stays correct on those shapes, but the whole column is still read: these rows pin that, so relaxing either exclusion flips exactly them.';
DROP TABLE IF EXISTS t_caj_skipidx;
DROP TABLE IF EXISTS t_caj_pk;
DROP TABLE IF EXISTS t_caj_final;
CREATE TABLE t_caj_skipidx (id UInt64, arr Array(UInt64), INDEX ix_arr arr TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_caj_skipidx SELECT number, range(number % 4) FROM numbers(20);
SELECT count() FROM t_caj_skipidx ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_skipidx ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_skipidx ARRAY JOIN arr;
-- The ARRAY JOIN is gone (this pass fired) but the fold did not happen, so no arr.size0 appears.
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_skipidx ARRAY JOIN arr) WHERE explain ILIKE '%ARRAY JOIN%';
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_skipidx ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_skipidx ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
-- The primary key reaches the same exclusion.
CREATE TABLE t_caj_pk (arr Array(UInt64)) ENGINE = MergeTree ORDER BY arr;
INSERT INTO t_caj_pk SELECT range(number % 4) FROM numbers(20);
SELECT count() FROM t_caj_pk ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_pk ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_pk ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_pk ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_pk ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
-- FINAL: the fold returns early, and the count must still match the deduplicated rows.
CREATE TABLE t_caj_final (id UInt64, arr Array(UInt64)) ENGINE = ReplacingMergeTree ORDER BY id;
INSERT INTO t_caj_final SELECT number, range(number % 4) FROM numbers(20);
INSERT INTO t_caj_final SELECT number, range(number % 3) FROM numbers(20);
SELECT count() FROM t_caj_final FINAL ARRAY JOIN arr;
SELECT count() = (SELECT count() FROM t_caj_final FINAL ARRAY JOIN arr SETTINGS optimize_functions_to_subcolumns = 0) FROM t_caj_final FINAL ARRAY JOIN arr;
SELECT count() > 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_final FINAL ARRAY JOIN arr) WHERE explain ILIKE '%sum(length%';
SELECT count() = 0 FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM t_caj_final FINAL ARRAY JOIN arr) WHERE explain ILIKE '%arr.size0%';
DROP TABLE t_caj_final;
DROP TABLE t_caj_pk;
DROP TABLE t_caj_skipidx;

DROP TABLE t_count_aj;
