-- Tags: no-fasttest
-- no-fasttest: requires the JSON data type.

-- Regression test for the ColumnObject (JSON) shared-data comparison optimization: sorting a JSON
-- column whose paths live in shared data used to materialize a temporary ColumnDynamic and run a
-- full binary deserialization for BOTH sides of every comparison. This pins the comparison ORDER
-- so it stays identical to the materializing implementation, and guards that ORDER BY over a
-- shared-data-heavy JSON column completes quickly instead of spending minutes in the compare storm.

SET enable_json_type = 1;

-- 1. All paths in shared data (max_dynamic_paths = 0): the optimized both-SHARED_DATA path.
DROP TABLE IF EXISTS t_json_shared;
CREATE TABLE t_json_shared (id UInt32, json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_shared VALUES
    (1,  '{"a": 1}'),                    -- Int64
    (2,  '{"a": "1"}'),                   -- String (same path, different type -> type-name order)
    (3,  '{"a": 2}'),
    (4,  '{"a": 1.5}'),                   -- Float64
    (5,  '{"a": true}'),                  -- Bool
    (6,  '{"a": 1, "b": 2}'),             -- superset of {"a"}
    (7,  '{"a": 1, "b": 3}'),
    (8,  '{"b": 2}'),                     -- different path set
    (9,  '{}'),                           -- empty (no paths)
    (10, '{"a": null}'),                  -- explicit null == absent path
    (11, '{"a": 1, "c": {"d": 5}}'),      -- nested object
    (12, '{"a": 1, "c": {"d": 4}}'),
    (13, '{"a": [1, 2, 3]}'),             -- array value
    (14, '{"a": [1, 2]}'),
    (15, '{"a": 1}');                     -- duplicate of row 1 (byte-equality fast path)

SELECT 'asc';
SELECT id, json FROM t_json_shared ORDER BY json ASC, id ASC;
SELECT 'desc';
SELECT id, json FROM t_json_shared ORDER BY json DESC, id ASC;

-- Equality / DISTINCT rely on compare()==0; the byte-equality fast path must return 0 only for
-- identical serializations (rows 1, 15 and their reordering with row 3).
SELECT 'distinct';
SELECT DISTINCT json FROM t_json_shared ORDER BY json, toString(json);

DROP TABLE t_json_shared;

-- 2. Cross-part merge over shared-data JSON: forces the both-SHARED comparison across two
-- ColumnObject instances (MergingSortedAlgorithm), not just an in-block sort.
DROP TABLE IF EXISTS t_json_shared_parts;
CREATE TABLE t_json_shared_parts (id UInt32, json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_shared_parts VALUES (1, '{"a": "3"}'), (2, '{"a": "1"}'), (3, '{"a": "2", "b": "9"}');
INSERT INTO t_json_shared_parts VALUES (4, '{"a": "2"}'), (5, '{"a": "3", "c": "1"}'), (6, '{"a": "1"}');
SELECT 'merge';
SELECT id, json FROM t_json_shared_parts ORDER BY json ASC, id ASC SETTINGS max_threads = 2, max_block_size = 2;
DROP TABLE t_json_shared_parts;

-- 3. Mixed structure (max_dynamic_paths = 2): some paths dynamic, some shared, multiple parts.
-- Exercises the UNCHANGED typed/dynamic compare path together with the optimized shared path.
DROP TABLE IF EXISTS t_json_mixed;
CREATE TABLE t_json_mixed (id UInt32, json JSON(max_dynamic_paths = 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_mixed VALUES
    (1, '{"a": 1, "b": 2}'), (2, '{"a": 1, "b": 3}'), (3, '{"a": 2, "c": "x"}'),
    (4, '{"a": 2, "c": "y", "d": 5}'), (5, '{"a": "s", "b": 1}'), (6, '{"a": 1.5}'), (7, '{}');
INSERT INTO t_json_mixed VALUES
    (8, '{"a": 1, "f": 1, "g": 2, "h": 3}'), (9, '{"a": 1, "f": 1, "g": 2, "h": 4}'),
    (10, '{"a": 3}'), (11, '{"a": 1, "b": null}'), (12, '{"b": 2}');
SELECT 'mixed';
SELECT id, json FROM t_json_mixed ORDER BY json ASC, id ASC;
DROP TABLE t_json_mixed;

-- 4. Performance regression guard: sorting a shared-data-heavy JSON column that shares a long path
-- prefix (so each comparison walks deep, the worst case) must complete quickly. Pre-fix this took
-- minutes under debug/sanitizer builds (the compare storm); post-fix it is well under a second.
-- ORDER BY ... FORMAT Null runs the full sort and discards the output; we only assert it finishes
-- (the test times out otherwise). max_threads is pinned so timing does not depend on the randomized
-- thread count.
DROP TABLE IF EXISTS t_json_perf;
CREATE TABLE t_json_perf (json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_perf
SELECT toJSONString(mapFromArrays(
    arrayMap(i -> 'p' || toString(i), range(8)),
    arrayMap(i -> if(i < 7, 'shared_prefix', toString(number)), range(8))))
FROM numbers(50000);
SELECT 'perf';
SELECT json FROM t_json_perf ORDER BY json FORMAT Null SETTINGS max_threads = 4;
DROP TABLE t_json_perf;
