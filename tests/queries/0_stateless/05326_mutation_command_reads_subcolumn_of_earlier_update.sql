-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutations this test needs to stay pending

-- A command of a mutation that reads a subcolumn of a column rewritten by an earlier command of the same
-- mutation must see the rewritten value, as it does when the two commands run as separate mutations.

SELECT 'subcolumn kinds';
DROP TABLE IF EXISTS t_kinds;
CREATE TABLE t_kinds
(
    id UInt8,
    a Array(UInt32),
    tu Tuple(p UInt32, q UInt32),
    x Nullable(UInt32),
    mp Map(String, UInt32),
    v Variant(String, UInt32),
    j JSON,
    d Dynamic,
    n Nested(e UInt32, f UInt32),
    k1 UInt64, k2 UInt64, k3 UInt8, k4 UInt64, k5 Nullable(UInt32), k6 String, k7 Nullable(UInt64), k8 UInt64,
    c1 UInt64, c2 UInt64, c3 UInt8
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_kinds (id, a, tu, x, mp, v, j, d, `n.e`, `n.f`) VALUES
    (1, [1, 2], (1, 2), NULL, map('a', 1), 's', '{"f":1}', 's', [1], [2]),
    (2, [3], (3, 4), 5, map('b', 2, 'c', 3), 5::UInt32, '{"f":3}', 5::UInt64, [3, 4], [5, 6]);
ALTER TABLE t_kinds
    UPDATE a = [7, 8, 9], tu = (10, 20), x = 7, mp = map('x', 1, 'y', 2, 'z', 3), v = 7::UInt32, j = '{"f":10}', d = 7::UInt64,
        `n.e` = [7, 8, 9], `n.f` = [1, 1, 1] WHERE id = 1,
    UPDATE k1 = a.size0, k2 = tu.p, k3 = x.null, k4 = length(mp.keys), k5 = v.UInt32, k6 = toString(j.f), k7 = d.UInt64,
        k8 = n.e.size0, c1 = length(a), c2 = tupleElement(tu, 'p'), c3 = isNull(x) WHERE 1
    SETTINGS mutations_sync = 2;
SELECT id, k1, k2, k3, k4, k5, k6, k7, k8, c1, c2, c3 FROM t_kinds ORDER BY id;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_kinds' AND active;

SELECT 'compact part';
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (id UInt8, a Array(UInt32), tu Tuple(p UInt32, q UInt32), b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact VALUES (1, [1, 2], (1, 2), 0, 0), (2, [3], (3, 4), 0, 0);
ALTER TABLE t_compact UPDATE a = [7, 8, 9], tu = (10, 20) WHERE id = 1, UPDATE b = a.size0, c = tu.p WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, b, c FROM t_compact ORDER BY id;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;

SELECT 'predicates';
DROP TABLE IF EXISTS t_predicates;
CREATE TABLE t_predicates (id UInt8, a Array(UInt32), b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_predicates VALUES (1, [1, 2], 0), (2, [3], 0), (3, [4, 5], 0);
ALTER TABLE t_predicates UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = 1 WHERE a.size0 = 3, UPDATE a = [1, 1, 1] WHERE id = 3, DELETE WHERE a.size0 = 2
    SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t_predicates ORDER BY id;

SELECT 'delete and update in one stage';
DROP TABLE IF EXISTS t_stage;
CREATE TABLE t_stage (id UInt8, a Array(UInt32), b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_stage VALUES (1, [1, 2], 0), (2, [3], 0), (3, [4, 5], 0);
ALTER TABLE t_stage UPDATE a = [7, 8, 9] WHERE id = 1, DELETE WHERE a.size0 = 2, UPDATE b = a.size0 WHERE 1
    SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t_stage ORDER BY id;

SELECT 'materialized dependent';
DROP TABLE IF EXISTS t_materialized;
CREATE TABLE t_materialized (id UInt8, a Array(UInt32), m Array(UInt32) MATERIALIZED a, b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialized (id, a, b) VALUES (1, [1, 2], 0), (2, [3], 0);
ALTER TABLE t_materialized UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = m.size0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, m, b FROM t_materialized ORDER BY id;

SELECT 'order of commands';
DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order (id UInt8, a Array(UInt32), b UInt64, c UInt64, e UInt64, f UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_order VALUES (1, [1, 2], 0, 0, 0, 0), (2, [3], 0, 0, 0, 0);
-- b reads `a` before it is rewritten, e in the same command that rewrites it; c through an unrelated command in between.
ALTER TABLE t_order
    UPDATE b = a.size0 WHERE 1,
    UPDATE a = [7, 8, 9] WHERE id = 1,
    UPDATE f = 5 WHERE 1,
    UPDATE c = a.size0 WHERE 1,
    UPDATE a = [1, 1, 1, 1], e = a.size0 WHERE id = 2
    SETTINGS mutations_sync = 2;
SELECT id, a, b, c, e, f FROM t_order ORDER BY id;

SELECT 'two mutations';
DROP TABLE IF EXISTS t_two;
CREATE TABLE t_two (id UInt8, a Array(UInt32), tu Tuple(p UInt32, q UInt32), b UInt64, c UInt64, y UInt8) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_two VALUES (1, [1, 2], (1, 2), 0, 0, 0), (2, [3], (3, 4), 0, 0, 0);
SYSTEM STOP MERGES t_two;
ALTER TABLE t_two UPDATE a = [7, 8, 9], tu = (10, 20) WHERE id = 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_two UPDATE b = a.size0, c = tu.p WHERE 1 SETTINGS mutations_sync = 0;
SELECT 'pending', id, a, tu, b, c FROM t_two ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SYSTEM START MERGES t_two;
ALTER TABLE t_two UPDATE y = 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, a, tu, b, c FROM t_two ORDER BY id SETTINGS apply_mutations_on_fly = 0;

SELECT 'alias column';
DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias (id UInt8, a Array(UInt32), al UInt64 ALIAS a.size0, al2 UInt64 ALIAS al * 10, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_alias (id, a, b, c) VALUES (1, [1, 2], 0, 0), (2, [3], 0, 0), (3, [4, 5], 0, 0);
ALTER TABLE t_alias UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = al, c = al2 WHERE 1, DELETE WHERE al = 2
    SETTINGS mutations_sync = 2;
SELECT id, a, al, b, c FROM t_alias ORDER BY id;

SELECT 'unusual column names';
DROP TABLE IF EXISTS t_names;
CREATE TABLE t_names (id UInt8, a Array(UInt32), `'size0'_String` UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_names VALUES (1, [1, 2], 100, 0), (2, [3], 100, 0);
ALTER TABLE t_names UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = `'size0'_String` + a.size0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t_names ORDER BY id;
DROP TABLE IF EXISTS t_names2;
CREATE TABLE t_names2 (id UInt8, a Array(UInt32), `getSubcolumn(a, 'size0'_String)` UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_names2 VALUES (1, [1, 2], 200, 0), (2, [3], 200, 0);
ALTER TABLE t_names2 UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = `getSubcolumn(a, 'size0'_String)` + a.size0 WHERE 1
    SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t_names2 ORDER BY id;

SELECT 'memory engine';
DROP TABLE IF EXISTS t_memory;
CREATE TABLE t_memory (id UInt8, a Array(UInt32), b UInt64) ENGINE = Memory;
INSERT INTO t_memory VALUES (1, [1, 2], 0), (2, [3], 0);
ALTER TABLE t_memory UPDATE a = [7, 8, 9] WHERE id = 1, UPDATE b = a.size0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, a, b FROM t_memory ORDER BY id;

DROP TABLE t_kinds;
DROP TABLE t_compact;
DROP TABLE t_predicates;
DROP TABLE t_stage;
DROP TABLE t_materialized;
DROP TABLE t_order;
DROP TABLE t_two;
DROP TABLE t_alias;
DROP TABLE t_names;
DROP TABLE t_names2;
DROP TABLE t_memory;
