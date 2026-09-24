-- Tags: no-random-merge-tree-settings

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET move_all_conditions_to_prewhere = 1;
-- The runtime filter cases below need the real cardinalities: clickhouse-test randomizes
-- query_plan_optimize_join_order_randomize to a nonzero value in 95% of runs, and the
-- substituted estimates both pick the join sides and decide whether a runtime filter is
-- planted at all (join_runtime_filter_min_probe_rows).
SET query_plan_optimize_join_order_randomize = 0;

-- A guard predicate and a potentially throwing predicate over subcolumns of the same physical
-- column must not share a PREWHERE read step, otherwise the throwing predicate is evaluated on
-- the rows the guard rejects.

DROP TABLE IF EXISTS t_prewhere_guard_json;
CREATE TABLE t_prewhere_guard_json (id Int64, session_id Int64, payload JSON, filler String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

-- 2/3 of the rows have coordinates, 1/3 are NULL and must be filtered by the guard.
-- Kept at 10000 rows: the cast to JSON dominates this test's CPU cost.
INSERT INTO t_prewhere_guard_json
SELECT number, number,
       if(number % 3 = 0, '{}', '{"longitude":4.35,"latitude":52.06}')::JSON,
       repeat('x', 200)
FROM numbers(10000);

SELECT 'json guard, default settings';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, both coordinates guarded';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL AND payload.latitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, allow_reorder_prewhere_conditions = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS allow_reorder_prewhere_conditions = 0;

SELECT 'json guard, query_plan_merge_filters = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_json;

-- The same shape with a guard that keeps most rows. ReadResult::optimize only applies a step
-- filter on its own when less than 60% of the rows pass, so this case additionally requires the
-- preceding step to force filter materialization.
DROP TABLE IF EXISTS t_prewhere_guard_json_low_sel;
CREATE TABLE t_prewhere_guard_json_low_sel (id Int64, session_id Int64, payload JSON, filler String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_prewhere_guard_json_low_sel
SELECT number, number,
       if(number % 10 = 0, '{}', '{"longitude":4.35,"latitude":52.06}')::JSON,
       repeat('x', 200)
FROM numbers(10000);

SELECT 'json guard, 90% of rows pass the guard';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json_low_sel
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, 90% of rows pass the guard, query_plan_merge_filters = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json_low_sel
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_json_low_sel;

-- The same invariant for Map subcolumns, including a user written PREWHERE.
DROP TABLE IF EXISTS t_prewhere_guard_map;
CREATE TABLE t_prewhere_guard_map (id Int64, tags Map(String, String), filler String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_prewhere_guard_map
SELECT number,
       if(number % 3 = 0, map('safe', '', 'val', 'not a number'), map('safe', 'y', 'val', toString(number % 100))),
       repeat('x', 200)
FROM numbers(100000);

SELECT 'map guard, WHERE';
SELECT count() FROM t_prewhere_guard_map WHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50;

SELECT 'map guard, explicit PREWHERE';
SELECT count() FROM t_prewhere_guard_map PREWHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50;

SELECT 'map guard, WHERE, query_plan_merge_filters = 0';
SELECT count() FROM t_prewhere_guard_map WHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_map;

-- The guard and the throwing condition may also read different physical columns, so they land in
-- separate steps for a reason unrelated to safety. A step does not filter the block it hands over,
-- so the preceding step must still be asked to materialize its filter.
--
-- Only a user written PREWHERE is asserted here. A flat WHERE gives no evaluation order guarantee:
-- MergeTreeWhereOptimizer orders the conditions it moves by estimated column size, so the throwing
-- condition can legitimately become the first step, with nothing before it to filter. That happens
-- on this table when `gate` is stored sparsely, which is why the sparse serialization threshold is
-- pinned below.
DROP TABLE IF EXISTS t_prewhere_guard_mixed;
CREATE TABLE t_prewhere_guard_mixed (id Int64, gate UInt8, tags Map(String, String), filler String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

-- 90% of the rows pass the guard, which is above the threshold at which a read step applies its
-- filter on its own.
INSERT INTO t_prewhere_guard_mixed
SELECT number,
       if(number % 10 = 0, 0, 1),
       if(number % 10 = 0, map('val', 'not a number'), map('val', toString(number % 100))),
       repeat('x', 200)
FROM numbers(100000);

SELECT 'mixed storage columns, explicit PREWHERE';
SELECT count() FROM t_prewhere_guard_mixed PREWHERE gate = 1 AND toUInt64(tags['val']) > 50;

SELECT 'mixed storage columns, explicit PREWHERE, query_plan_merge_filters = 0';
SELECT count() FROM t_prewhere_guard_mixed PREWHERE gate = 1 AND toUInt64(tags['val']) > 50
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_mixed;

-- A WHERE moved into an existing PREWHERE is combined as `and(existing, and(guard, throwing))`, so
-- the condition list is a nested conjunction. It has to be flattened before grouping, otherwise the
-- inner AND is one condition and the guard shares a step with the throwing conversion again.
DROP TABLE IF EXISTS t_prewhere_guard_nested;
CREATE TABLE t_prewhere_guard_nested (id UInt64, pre UInt8, tags Map(String, String), filler String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

-- 90% of the rows pass the guard, above the threshold at which a read step applies its filter by itself.
INSERT INTO t_prewhere_guard_nested
SELECT number, 1,
       if(number % 10 = 0, map('safe', '', 'val', 'not a number'), map('safe', 'y', 'val', toString(number % 100))),
       repeat('x', 200)
FROM numbers(100000);

-- The converted value is also selected, so the conversion is an output of the PREWHERE DAG and
-- short circuit execution cannot skip it.
SELECT 'nested conjunction, WHERE moved into an existing PREWHERE';
SELECT count(), sum(value) FROM (
    SELECT toUInt64(tags['val']) AS value
    FROM t_prewhere_guard_nested
    PREWHERE pre = 1
    WHERE tags['safe'] != '' AND value > 50
)
SETTINGS optimize_prewhere_after_pushdown = 1;

SELECT 'nested conjunction, WHERE moved into an existing PREWHERE, query_plan_merge_filters = 0';
SELECT count(), sum(value) FROM (
    SELECT toUInt64(tags['val']) AS value
    FROM t_prewhere_guard_nested
    PREWHERE pre = 1
    WHERE tags['safe'] != '' AND value > 50
)
SETTINGS optimize_prewhere_after_pushdown = 1, query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_nested;

-- Conditions that cannot throw are still rewritten to subcolumns of one physical column.
-- That they also share a single read step is asserted in the .sh companion of this test, which can
-- count read steps through ProfileEvents.
DROP TABLE IF EXISTS t_prewhere_group_map;
CREATE TABLE t_prewhere_group_map (id UInt64, tags Map(String, String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_prewhere_group_map
SELECT number, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(4)), arrayMap(i -> toString(number + i), range(4)))
FROM numbers(1000);

SELECT 'non throwing map conditions are rewritten to subcolumns';
-- enable_parallel_replicas = 0 keeps the plan local: otherwise a ReadFromRemoteParallelReplicas
-- step embeds the whole remote query in its description and matches the filter a second time.
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_prewhere_group_map
    PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != ''
    SETTINGS enable_parallel_replicas = 0
) WHERE explain ILIKE '%tags.key_k0%' AND explain ILIKE '%tags.key_k3%';

SELECT count() FROM t_prewhere_group_map
PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != '';

DROP TABLE t_prewhere_group_map;

-- A runtime join filter is pushed into the probe side's PREWHERE next to the subquery's own guard,
-- so the join key expression must not be evaluated on the rows that guard rejects.

DROP TABLE IF EXISTS t_prewhere_guard_rf_probe;
DROP TABLE IF EXISTS t_prewhere_guard_rf_build;
CREATE TABLE t_prewhere_guard_rf_probe (c1 Nullable(String), c2 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_prewhere_guard_rf_build (c1 String, c2 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_guard_rf_probe SELECT if(number % 3 = 0, NULL, toString(number % 100)), 1 FROM numbers(1000);
INSERT INTO t_prewhere_guard_rf_build SELECT toString(number), 1 FROM numbers(50);

SELECT 'runtime filter, cast guarded by IS NOT NULL';
-- join_runtime_filter_min_probe_rows is pinned because at its default of 1000 this probe side is
-- estimated at 666 rows, no runtime filter is planted at all, and the query would pass for an
-- unrelated reason.
SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_probe WHERE c1 IS NOT NULL) AS a
INNER JOIN t_prewhere_guard_rf_build AS b ON b.c2 = a.c2 AND b.c1 = a.k
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;

SELECT 'runtime filter, cast guarded by IS NOT NULL, runtime filters off';
SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_probe WHERE c1 IS NOT NULL) AS a
INNER JOIN t_prewhere_guard_rf_build AS b ON b.c2 = a.c2 AND b.c1 = a.k
SETTINGS enable_join_runtime_filters = 0;

SELECT 'runtime filter reaches the probe side read';
-- Liveness assertion for the two arms above. They assert values that a plan with no runtime filter
-- at all also produces, so they would stay green if the filter stopped being planted or stopped
-- reaching the read, and would then cover nothing. The annotation names the guarded CAST as the
-- filter key, which is exactly the route these arms exercise. enable_parallel_replicas = 0 keeps
-- the plan local, as in the map assertion above.
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_probe WHERE c1 IS NOT NULL) AS a
    INNER JOIN t_prewhere_guard_rf_build AS b ON b.c2 = a.c2 AND b.c1 = a.k
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Runtime filters:%' AND explain ILIKE '%CAST(c1%';

SELECT 'runtime filter absent when the feature is off';
-- Negative control for the assertion above: it must be the runtime filter that makes the pattern
-- match, not some line the plan prints either way.
SELECT count() = 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_probe WHERE c1 IS NOT NULL) AS a
    INNER JOIN t_prewhere_guard_rf_build AS b ON b.c2 = a.c2 AND b.c1 = a.k
    SETTINGS enable_join_runtime_filters = 0, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Runtime filters:%' AND explain ILIKE '%CAST(c1%';

SELECT 'guarded cast selected as an output, no join';
-- The cast is only evaluated inside the read when it is a required output of it: the same query
-- under an aggregate (count(), max(k)) never reaches the defect.
SELECT k, c2 FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_probe WHERE c1 IS NOT NULL) AS a
WHERE a.k > '' ORDER BY k, c2 LIMIT 1;

DROP TABLE t_prewhere_guard_rf_probe;
DROP TABLE t_prewhere_guard_rf_build;

-- The guard is on the node, not on the type: a LowCardinality(Nullable(...)) key behaves the same.
DROP TABLE IF EXISTS t_prewhere_guard_rf_lc;
CREATE TABLE t_prewhere_guard_rf_lc (c1 LowCardinality(Nullable(String)), c2 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_guard_rf_lc SELECT if(number % 3 = 0, NULL, toString(number % 100)), 1 FROM numbers(100000);

SELECT 'runtime filter, LowCardinality(Nullable) key';
SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_lc WHERE c1 IS NOT NULL) AS a
INNER JOIN (SELECT toString(number) AS c1, 1 AS c2 FROM numbers(50)) AS b ON b.c2 = a.c2 AND b.c1 = a.k
SETTINGS enable_join_runtime_filters = 1;

SELECT 'runtime filter reaches the LowCardinality probe side read';
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT CAST(c1, 'String') AS k, c2 FROM t_prewhere_guard_rf_lc WHERE c1 IS NOT NULL) AS a
    INNER JOIN (SELECT toString(number) AS c1, 1 AS c2 FROM numbers(50)) AS b ON b.c2 = a.c2 AND b.c1 = a.k
    SETTINGS enable_join_runtime_filters = 1, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Runtime filters:%' AND explain ILIKE '%CAST(c1%';

DROP TABLE t_prewhere_guard_rf_lc;

-- The same defect with a parsing conversion instead of a NULL cast: toUInt64(substring(...)) on the
-- rows that `LIKE 'nmf-%'` rejects fails with CANNOT_PARSE_TEXT.
DROP TABLE IF EXISTS t_prewhere_guard_rf_parse_build;
DROP TABLE IF EXISTS t_prewhere_guard_rf_parse_probe;
CREATE TABLE t_prewhere_guard_rf_parse_build (c1 UInt64, c2 Float64) ENGINE = MergeTree ORDER BY c1;
CREATE TABLE t_prewhere_guard_rf_parse_probe (c1 String, c2 Float64) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_prewhere_guard_rf_parse_build SELECT number, number % 1000 FROM numbers(200000);
INSERT INTO t_prewhere_guard_rf_parse_probe
SELECT if(number % 4 = 0, concat('nmf-', toString(number)), lower(hex(MD5(toString(number))))), number % 1000
FROM numbers(200000);

SELECT 'runtime filter, parsing conversion guarded by LIKE';
SELECT count(), sum(h.c2 = t.c2)
FROM (SELECT toUInt64(substring(c1, 5)) AS a1, c2 FROM t_prewhere_guard_rf_parse_probe WHERE c1 LIKE 'nmf-%') AS h
INNER JOIN (SELECT c1, c2 FROM t_prewhere_guard_rf_parse_build WHERE c1 >= 50000 AND c1 <= 60000) AS t ON t.c1 = h.a1
SETTINGS enable_join_runtime_filters = 1;

SELECT 'runtime filter, parsing conversion guarded by LIKE, runtime filters off';
SELECT count(), sum(h.c2 = t.c2)
FROM (SELECT toUInt64(substring(c1, 5)) AS a1, c2 FROM t_prewhere_guard_rf_parse_probe WHERE c1 LIKE 'nmf-%') AS h
INNER JOIN (SELECT c1, c2 FROM t_prewhere_guard_rf_parse_build WHERE c1 >= 50000 AND c1 <= 60000) AS t ON t.c1 = h.a1
SETTINGS enable_join_runtime_filters = 0;

SELECT 'runtime filter reaches the parsing conversion probe side read';
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT count(), sum(h.c2 = t.c2)
    FROM (SELECT toUInt64(substring(c1, 5)) AS a1, c2 FROM t_prewhere_guard_rf_parse_probe WHERE c1 LIKE 'nmf-%') AS h
    INNER JOIN (SELECT c1, c2 FROM t_prewhere_guard_rf_parse_build WHERE c1 >= 50000 AND c1 <= 60000) AS t ON t.c1 = h.a1
    SETTINGS enable_join_runtime_filters = 1, enable_parallel_replicas = 0
) WHERE explain ILIKE '%Runtime filters:%' AND explain ILIKE '%toUInt64(substring(c1%';

DROP TABLE t_prewhere_guard_rf_parse_build;
DROP TABLE t_prewhere_guard_rf_parse_probe;
