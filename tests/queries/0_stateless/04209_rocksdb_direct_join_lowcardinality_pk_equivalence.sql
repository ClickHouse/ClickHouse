-- Tags: use-rocksdb
-- Coverage: exercises the `recursiveRemoveLowCardinality` equivalence stripping in
-- `tryDirectJoin` at src/Planner/PlannerJoins.cpp:1108-1111. The PR
-- (STID 3436-4175) added a type check between the right join key and the storage
-- primary key; the check strips `Nullable` AND `LowCardinality` wrappers on both
-- sides "to match the equivalence semantics used by getByKeys itself."
--
-- The PR's own tests (04201_direct_join_type_mismatch_rocksdb.sql) only exercise
-- the `removeNullable` path — none use `LowCardinality` wrappers, leaving the
-- `recursiveRemoveLowCardinality` calls completely untested.
--
-- Scenario: storage PK is `LowCardinality(String)`, right join key is plain
-- `String`. The legacy `JOIN ... USING` planner casts both sides to the
-- common supertype `String`, so at the time `tryDirectJoin` runs:
--     right_table_expression_header.key.type == String
--     storage->getSampleBlock().key.type    == LowCardinality(String)
--
-- These two types are equivalent for `getByKeys` (which strips `LowCardinality`
-- via `recursiveRemoveLowCardinality`), so `DirectKeyValueJoin` MUST still be
-- chosen — fall-back to `HashJoin` here would be a silent performance regression
-- on every key-value lookup query that uses a `LowCardinality` PK.
--
-- Mutation guard: drop `recursiveRemoveLowCardinality` from the storage-side
-- type computation at PlannerJoins.cpp:1110 and the EXPLAIN assertions below
-- flip from "Algorithm: DirectKeyValueJoin" to a HashJoin/JOIN line.

DROP TABLE IF EXISTS rdb_lc_pk;
DROP TABLE IF EXISTS t_str_right;

CREATE TABLE rdb_lc_pk (key LowCardinality(String), value String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb_lc_pk VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

CREATE TABLE t_str_right (k String) ENGINE = TinyLog;
INSERT INTO t_str_right VALUES ('a'), ('b'), ('c'), ('d');

-- INNER JOIN: legacy planner must keep DirectKeyValueJoin despite
-- LowCardinality(String) PK vs plain String right key (post-USING-cast).
SELECT 'inner join legacy planner result';
SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
INNER JOIN rdb_lc_pk USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

SELECT 'inner join legacy planner explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
    INNER JOIN rdb_lc_pk USING (key)
    SETTINGS query_plan_use_new_logical_join_step = 0
)
WHERE explain LIKE '%Algorithm:%';

-- LEFT JOIN: same equivalence path, different `allowed_left` strictness branch.
SELECT 'left join legacy planner result';
SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
LEFT JOIN rdb_lc_pk USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

SELECT 'left join legacy planner explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str_right) AS t
    LEFT JOIN rdb_lc_pk USING (key)
    SETTINGS query_plan_use_new_logical_join_step = 0
)
WHERE explain LIKE '%Algorithm:%';

DROP TABLE rdb_lc_pk;
DROP TABLE t_str_right;
