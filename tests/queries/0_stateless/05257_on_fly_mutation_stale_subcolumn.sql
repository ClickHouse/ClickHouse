-- Tags: no-shared-catalog
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will
-- continue to merge and can materialize the mutation this test needs to stay pending

-- A subcolumn that is stored in the part must answer with the pending UPDATE of its parent, and
-- must give the same answer as it does once that mutation is materialized. Reading it straight
-- from the part returns the pre-update value while the parent in the same SELECT is updated.

SET alter_sync = 0, mutations_sync = 0;
SET apply_mutations_on_fly = 1;

SELECT 'array, nullable and tuple subcolumns';

DROP TABLE IF EXISTS t_stale_subcolumn;
CREATE TABLE t_stale_subcolumn (id UInt8, a Array(UInt32), n Nullable(Int32), tup Tuple(s String, x UInt8), y UInt8)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G';
INSERT INTO t_stale_subcolumn VALUES (1, [1, 2], 1, ('old', 1), 0), (2, [1], 5, ('older', 2), 0);
SYSTEM STOP MERGES t_stale_subcolumn;
ALTER TABLE t_stale_subcolumn UPDATE a = [7, 8, 9], n = NULL, tup = ('new', 9) WHERE 1;
SELECT 'pending', id, a, a.size0, length(a), n, n.null, isNull(n), tup.s FROM t_stale_subcolumn ORDER BY id;
SELECT 'pending, prewhere', id FROM t_stale_subcolumn PREWHERE length(a) = 3 ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn;
ALTER TABLE t_stale_subcolumn UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, a, a.size0, length(a), n, n.null, isNull(n), tup.s FROM t_stale_subcolumn ORDER BY id;
SELECT 'materialized, prewhere', id FROM t_stale_subcolumn PREWHERE length(a) = 3 ORDER BY id;

SELECT 'the same in a compact part';

DROP TABLE IF EXISTS t_stale_subcolumn_compact;
CREATE TABLE t_stale_subcolumn_compact (id UInt8, a Array(UInt32), n Nullable(Int32), tup Tuple(s String, x UInt8), y UInt8)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_stale_subcolumn_compact VALUES (1, [1, 2], 1, ('old', 1), 0), (2, [1], 5, ('older', 2), 0);
SYSTEM STOP MERGES t_stale_subcolumn_compact;
ALTER TABLE t_stale_subcolumn_compact UPDATE a = [7, 8, 9], n = NULL, tup = ('new', 9) WHERE 1;
SELECT 'pending', id, a.size0, n.null, tup.s FROM t_stale_subcolumn_compact ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn_compact;
ALTER TABLE t_stale_subcolumn_compact UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, a.size0, n.null, tup.s FROM t_stale_subcolumn_compact ORDER BY id;

SELECT 'map and json subcolumns';

DROP TABLE IF EXISTS t_stale_subcolumn_map;
SET enable_json_type = 1;
CREATE TABLE t_stale_subcolumn_map (id UInt8, m Map(String, UInt32), data JSON, y UInt8)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_stale_subcolumn_map VALUES (1, map('k', 1), '{"f":"secret"}', 0);
SYSTEM STOP MERGES t_stale_subcolumn_map;
ALTER TABLE t_stale_subcolumn_map UPDATE m = map('z', 5), data = CAST('{"f":"public"}', 'JSON') WHERE 1;
SELECT 'pending', id, m.keys, m.values, data.f FROM t_stale_subcolumn_map ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn_map;
ALTER TABLE t_stale_subcolumn_map UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, m.keys, m.values, data.f FROM t_stale_subcolumn_map ORDER BY id;

SELECT 'a subcolumn that a later command reads is left to the mutation chain';

DROP TABLE IF EXISTS t_stale_subcolumn_chain;
CREATE TABLE t_stale_subcolumn_chain (id UInt8, a Array(UInt32), b UInt64, y UInt8)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_stale_subcolumn_chain VALUES (1, [1, 2], 0, 0);
SYSTEM STOP MERGES t_stale_subcolumn_chain;
ALTER TABLE t_stale_subcolumn_chain UPDATE a = [7, 8, 9] WHERE 1;
ALTER TABLE t_stale_subcolumn_chain UPDATE b = a.size0 WHERE 1;
SELECT 'pending', id, b FROM t_stale_subcolumn_chain ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn_chain;
ALTER TABLE t_stale_subcolumn_chain UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, b FROM t_stale_subcolumn_chain ORDER BY id;

SELECT 'with a lightweight delete pending as well';

DROP TABLE IF EXISTS t_stale_subcolumn_delete;
CREATE TABLE t_stale_subcolumn_delete (id UInt8, a Array(UInt32), y UInt8)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G';
INSERT INTO t_stale_subcolumn_delete VALUES (1, [1, 2], 0), (2, [1], 0);
SYSTEM STOP MERGES t_stale_subcolumn_delete;
SET lightweight_deletes_sync = 0;
DELETE FROM t_stale_subcolumn_delete WHERE id = 2;
ALTER TABLE t_stale_subcolumn_delete UPDATE a = [7, 8, 9] WHERE 1;
SELECT 'pending', id, a.size0 FROM t_stale_subcolumn_delete ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn_delete;
SET lightweight_deletes_sync = 2;
ALTER TABLE t_stale_subcolumn_delete UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, a.size0 FROM t_stale_subcolumn_delete ORDER BY id;

SELECT 'a physical column whose name contains a dot keeps reading from the part';

DROP TABLE IF EXISTS t_stale_subcolumn_nested;
CREATE TABLE t_stale_subcolumn_nested (id UInt8, n Nested(a Int32), v UInt32, y UInt8)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '10G';
INSERT INTO t_stale_subcolumn_nested VALUES (1, [1, 2, 3], 10, 0);
SYSTEM STOP MERGES t_stale_subcolumn_nested;
ALTER TABLE t_stale_subcolumn_nested UPDATE v = 99 WHERE 1;
SELECT 'pending', id, length(n.a), n.a, v FROM t_stale_subcolumn_nested ORDER BY id;
SYSTEM START MERGES t_stale_subcolumn_nested;
ALTER TABLE t_stale_subcolumn_nested UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'materialized', id, length(n.a), n.a, v FROM t_stale_subcolumn_nested ORDER BY id;

DROP TABLE t_stale_subcolumn, t_stale_subcolumn_compact, t_stale_subcolumn_map,
    t_stale_subcolumn_chain, t_stale_subcolumn_delete, t_stale_subcolumn_nested;
