-- PR 5: Execution tests for ORDER BY col DEPENDS ON dep_col.
-- Verifies that Kahn's BFS topological sort produces correct ordering.

-- Simple chain: 3 depends on 2, 2 depends on 1 → unique order 1, 2, 3.
DROP TABLE IF EXISTS t_topo_chain;
CREATE TABLE t_topo_chain (id UInt32, deps Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_topo_chain VALUES (1, []), (2, [1]), (3, [2]);
SELECT id FROM t_topo_chain ORDER BY id DEPENDS ON deps;

-- Single row with no dependencies.
DROP TABLE IF EXISTS t_topo_single;
CREATE TABLE t_topo_single (id UInt32, deps Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_topo_single VALUES (42, []);
SELECT id FROM t_topo_single ORDER BY id DEPENDS ON deps;

-- No dependencies at all: all rows emitted (count check).
DROP TABLE IF EXISTS t_topo_nodeps;
CREATE TABLE t_topo_nodeps (id UInt32, deps Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_topo_nodeps VALUES (10, []), (20, []), (30, []);
SELECT count() FROM (SELECT id FROM t_topo_nodeps ORDER BY id DEPENDS ON deps);

-- Dependency on a key not present in the result set is silently ignored.
DROP TABLE IF EXISTS t_topo_missing;
CREATE TABLE t_topo_missing (id UInt32, deps Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_topo_missing VALUES (5, [99]), (6, [5]);
SELECT id FROM t_topo_missing ORDER BY id DEPENDS ON deps;

-- Cycle detection must throw BAD_ARGUMENTS.
DROP TABLE IF EXISTS t_topo_cycle;
CREATE TABLE t_topo_cycle (id UInt32, deps Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_topo_cycle VALUES (1, [2]), (2, [1]);
SELECT id FROM t_topo_cycle ORDER BY id DEPENDS ON deps; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_topo_chain;
DROP TABLE t_topo_single;
DROP TABLE t_topo_nodeps;
DROP TABLE t_topo_missing;
DROP TABLE t_topo_cycle;
