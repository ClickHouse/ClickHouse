-- Tags: no-parallel
--
-- Coverage matrix for `RemoveRedundantSemiJoinPass`.
--
-- Each test prints, in order:
--   1) the rows produced with the optimisation OFF
--   2) the rows produced with the optimisation ON  (must equal #1)
--   3) `EXPLAIN SYNTAX run_query_tree_passes = 1` with the optimisation OFF
--   4) `EXPLAIN SYNTAX run_query_tree_passes = 1` with the optimisation ON
-- so you can read off directly which LEFT SEMI / LEFT ANTI join was eliminated
-- (or that none was eliminated for the negative cases).

SET enable_analyzer = 1;

-- ============================================================================
-- Schema
-- ============================================================================
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS extras;
DROP TABLE IF EXISTS users_alt;
DROP TABLE IF EXISTS users_pre;
DROP TABLE IF EXISTS child_t;

CREATE TABLE users    (uid Int32, age Int32, name String, active UInt8) ENGINE = MergeTree() ORDER BY uid;
CREATE TABLE orders   (oid Int32, uid Int32, alt_uid Int32)             ENGINE = MergeTree() ORDER BY oid;
CREATE TABLE extras   (eid Int32, uid Int32)                            ENGINE = MergeTree() ORDER BY eid;
CREATE TABLE users_alt(uid Int32, id  Int32)                            ENGINE = MergeTree() ORDER BY uid;
CREATE TABLE users_pre(uid Int32, age Int32)                            ENGINE = MergeTree() ORDER BY uid;
CREATE TABLE child_t  (cid Int32, uid Int32)                            ENGINE = MergeTree() ORDER BY cid;

INSERT INTO users    VALUES (1,20,'Alice',1),(2,33,'Bob',1),(3,40,'Charlie',0),(4,25,'Dave',1),(5,33,'Eve',0);
INSERT INTO orders   VALUES (10,1,2),(11,2,3),(12,3,1),(13,99,100);
INSERT INTO extras   VALUES (100,1),(101,2),(102,999);
INSERT INTO users_alt VALUES (1,100),(2,200),(3,300);
INSERT INTO users_pre VALUES (1,20),(2,30),(3,40);
INSERT INTO child_t  VALUES (1,10);

-- ============================================================================
-- Cases that ARE optimised (expect: one fewer LEFT SEMI / LEFT ANTI join)
-- ============================================================================

SELECT '== T01: SEMI subset, drop the looser one ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T02: SEMI identical filters, drop one ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T03: ANTI subset, drop the stricter one ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT ANTI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT ANTI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT ANTI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT ANTI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T04: SEMI through thin subquery wrapper ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T05: SEMI key permutation in ON (a=b vs b=a) ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u3.uid = u1.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u3.uid = u1.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u3.uid = u1.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u3.uid = u1.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T06: SEMI three-way, only one redundant pair ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 30) u4 ON u1.uid = u4.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 30) u4 ON u1.uid = u4.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 30) u4 ON u1.uid = u4.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 30) u4 ON u1.uid = u4.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T33: SEMI multi-conjunct filters in different order, equivalent => drop one ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T34: SEMI multi-conjunct strict superset (reordered) => drop the looser one ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1 AND name = 'Bob') u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1 AND name = 'Bob') u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1 AND name = 'Bob') u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33 AND active = 1 AND name = 'Bob') u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1 AND age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T35: aliases everywhere (outer table alias, inner table alias, column alias on join key) ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT inner_u.uid AS my_uid, inner_u.age, inner_u.name, inner_u.active FROM users AS inner_u WHERE inner_u.age = 33) u3 ON u1.uid = u3.my_uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT inner_u.uid AS my_uid, inner_u.age, inner_u.name, inner_u.active FROM users AS inner_u WHERE inner_u.age = 33) u3 ON u1.uid = u3.my_uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT inner_u.uid AS my_uid, inner_u.age, inner_u.name, inner_u.active FROM users AS inner_u WHERE inner_u.age = 33) u3 ON u1.uid = u3.my_uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT inner_u.uid AS my_uid, inner_u.age, inner_u.name, inner_u.active FROM users AS inner_u WHERE inner_u.age = 33) u3 ON u1.uid = u3.my_uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

-- ============================================================================
-- Cases that are NOT optimised (expect: all LEFT SEMI / LEFT ANTI joins kept,
-- and EXPLAIN opt=0 / opt=1 must be identical)
-- ============================================================================

SELECT '== T07: different right tables, no elimination ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users  u  ON o.uid = u.uid
  LEFT SEMI JOIN extras e  ON o.uid = e.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users  u  ON o.uid = u.uid
  LEFT SEMI JOIN extras e  ON o.uid = e.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users  u  ON o.uid = u.uid
  LEFT SEMI JOIN extras e  ON o.uid = e.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users  u  ON o.uid = u.uid
  LEFT SEMI JOIN extras e  ON o.uid = e.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T08: different right physical columns, no elimination ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN users u3 ON u1.uid = u3.age
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN users u3 ON u1.uid = u3.age
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN users u3 ON u1.uid = u3.age
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN users u3 ON u1.uid = u3.age
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T09: different left key expressions, no elimination ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid     = u1.uid
  LEFT SEMI JOIN users u2 ON o.alt_uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid     = u1.uid
  LEFT SEMI JOIN users u2 ON o.alt_uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid     = u1.uid
  LEFT SEMI JOIN users u2 ON o.alt_uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid     = u1.uid
  LEFT SEMI JOIN users u2 ON o.alt_uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T10: SEMI mixed with ANTI on same table/key, no elimination ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T11: incomparable filters, no elimination ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE name = 'Bob') u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE name = 'Bob') u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE name = 'Bob') u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE active = 1) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE name = 'Bob') u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T12: range subsumption is not detected (AST-only check) ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 10) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 10) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 10) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 10) u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age > 20) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T13: right side has GROUP BY, skip ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid, sum(age) AS s FROM users GROUP BY uid HAVING s > 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid, sum(age) AS s FROM users GROUP BY uid HAVING s > 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid, sum(age) AS s FROM users GROUP BY uid HAVING s > 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid, sum(age) AS s FROM users GROUP BY uid HAVING s > 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T14: right side has DISTINCT, skip ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT DISTINCT uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT DISTINCT uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT DISTINCT uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT DISTINCT uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T15: right side has LIMIT, skip ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid FROM users LIMIT 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid FROM users LIMIT 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid FROM users LIMIT 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT uid FROM users LIMIT 10) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T16: right side projects a computed column, skip ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT age AS uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT age AS uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT age AS uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT age AS uid FROM users) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T17: right side wraps a JOIN, skip ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT a.uid FROM users a JOIN users b ON a.uid = b.uid) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT a.uid FROM users a JOIN users b ON a.uid = b.uid) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT a.uid FROM users a JOIN users b ON a.uid = b.uid) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT a.uid FROM users a JOIN users b ON a.uid = b.uid) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T18: right side has PREWHERE, must not be ignored ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN (SELECT * FROM users_pre WHERE age > 25) p1 ON o.uid = p1.uid
  LEFT SEMI JOIN (SELECT * FROM users_pre PREWHERE age > 35) p2 ON o.uid = p2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN (SELECT * FROM users_pre WHERE age > 25) p1 ON o.uid = p1.uid
  LEFT SEMI JOIN (SELECT * FROM users_pre PREWHERE age > 35) p2 ON o.uid = p2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN (SELECT * FROM users_pre WHERE age > 25) p1 ON o.uid = p1.uid
  LEFT SEMI JOIN (SELECT * FROM users_pre PREWHERE age > 35) p2 ON o.uid = p2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN (SELECT * FROM users_pre WHERE age > 25) p1 ON o.uid = p1.uid
  LEFT SEMI JOIN (SELECT * FROM users_pre PREWHERE age > 35) p2 ON o.uid = p2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T19: right column referenced in SELECT projection ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid, u2.name FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid, u2.name FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid, u2.name FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid, u2.name FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T20: right column referenced in WHERE ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  WHERE u2.age > 0
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  WHERE u2.age > 0
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  WHERE u2.age > 0
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  WHERE u2.age > 0
  ORDER BY u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T21: right column referenced in ORDER BY ==';
SELECT '-- rows  opt=0 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u2.name, u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u2.name, u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u2.name, u1.uid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u1.uid FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  ORDER BY u2.name, u1.uid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T22: right column referenced in GROUP BY/HAVING ==';
SELECT '-- rows  opt=0 --';
SELECT u2.name, count() AS c FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  GROUP BY u2.name HAVING c > 0
  ORDER BY u2.name SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT u2.name, count() AS c FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  GROUP BY u2.name HAVING c > 0
  ORDER BY u2.name SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u2.name, count() AS c FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  GROUP BY u2.name HAVING c > 0
  ORDER BY u2.name SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT u2.name, count() AS c FROM users u1
  LEFT SEMI JOIN users u2 ON u1.uid = u2.uid
  LEFT SEMI JOIN (SELECT * FROM users WHERE age = 33) u3 ON u1.uid = u3.uid
  GROUP BY u2.name HAVING c > 0
  ORDER BY u2.name SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T23: ANTI right column referenced via sibling JOIN ON ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT ANTI JOIN (SELECT * FROM users) u1 ON o.uid = u1.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE uid = 5) u2 ON o.uid = u2.uid
  LEFT JOIN child_t c ON c.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT ANTI JOIN (SELECT * FROM users) u1 ON o.uid = u1.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE uid = 5) u2 ON o.uid = u2.uid
  LEFT JOIN child_t c ON c.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT ANTI JOIN (SELECT * FROM users) u1 ON o.uid = u1.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE uid = 5) u2 ON o.uid = u2.uid
  LEFT JOIN child_t c ON c.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT ANTI JOIN (SELECT * FROM users) u1 ON o.uid = u1.uid
  LEFT ANTI JOIN (SELECT * FROM users WHERE uid = 5) u2 ON o.uid = u2.uid
  LEFT JOIN child_t c ON c.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T24: subquery renames a different physical column, must not be confused ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users_alt a1 ON o.uid = a1.uid
  LEFT SEMI JOIN (SELECT id AS uid FROM users_alt) a2 ON o.uid = a2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users_alt a1 ON o.uid = a1.uid
  LEFT SEMI JOIN (SELECT id AS uid FROM users_alt) a2 ON o.uid = a2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users_alt a1 ON o.uid = a1.uid
  LEFT SEMI JOIN (SELECT id AS uid FROM users_alt) a2 ON o.uid = a2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users_alt a1 ON o.uid = a1.uid
  LEFT SEMI JOIN (SELECT id AS uid FROM users_alt) a2 ON o.uid = a2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

-- ============================================================================
-- Cross-barrier safety: RIGHT/FULL JOIN between two SEMI/ANTI must block the
-- elimination in the unsafe direction (must NOT drop a join).
-- ============================================================================

SELECT '== T25: SEMI + RIGHT JOIN + SEMI, must NOT drop the OUTER (above-barrier) SEMI ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T26: SEMI + FULL JOIN + SEMI, must NOT drop the OUTER (above-barrier) SEMI ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T27: ANTI + RIGHT JOIN + ANTI, must NOT drop the INNER (below-barrier) ANTI ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T28: ANTI + FULL JOIN + ANTI, must NOT drop the INNER (below-barrier) ANTI ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN users u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

-- ============================================================================
-- Cross-barrier safe directions (must STILL be optimised)
-- ============================================================================

SELECT '== T29: SEMI + RIGHT JOIN + SEMI, dropping the INNER (below-barrier) SEMI is safe ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T30: ANTI + RIGHT JOIN + ANTI, dropping the OUTER (above-barrier) ANTI is safe ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  RIGHT JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T31: SEMI + INNER JOIN + SEMI, no barrier, still optimises ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  INNER JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  INNER JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  INNER JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  INNER JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T32: SEMI + LEFT JOIN + SEMI, no barrier, still optimises ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  LEFT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  LEFT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  LEFT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  LEFT JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T36: SEMI + FULL JOIN + SEMI, dropping the INNER (below-barrier) SEMI is safe ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT SEMI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT SEMI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

SELECT '== T37: ANTI + FULL JOIN + ANTI, dropping the OUTER (above-barrier) ANTI is safe ==';
SELECT '-- rows  opt=0 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- rows  opt=1 --';
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;
SELECT '-- query opt=0 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 0;
SELECT '-- query opt=1 --';
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT o.oid, e.eid AS ek FROM orders o
  LEFT ANTI JOIN users u1 ON o.uid = u1.uid
  FULL JOIN extras e ON o.uid = e.uid
  LEFT ANTI JOIN (SELECT uid FROM users WHERE uid = 1) u2 ON o.uid = u2.uid
  ORDER BY o.oid, e.eid SETTINGS optimize_remove_redundant_semi_join = 1;

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE users;
DROP TABLE orders;
DROP TABLE extras;
DROP TABLE users_alt;
DROP TABLE users_pre;
DROP TABLE child_t;
