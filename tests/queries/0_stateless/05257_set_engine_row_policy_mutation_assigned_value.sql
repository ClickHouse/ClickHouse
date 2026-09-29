-- A heavy mutation runs in the background with full access, so it is checked when it is submitted. The value
-- that an `UPDATE` assigns can read a view over a `Set` table with a row policy, which must be refused too.

DROP VIEW IF EXISTS v_rp;
DROP TABLE IF EXISTS set_rp;
DROP TABLE IF EXISTS mt_rp;

CREATE TABLE set_rp (k UInt64) ENGINE = Set;
INSERT INTO set_rp VALUES (1), (2);
CREATE VIEW v_rp SQL SECURITY INVOKER AS SELECT number FROM numbers(10) WHERE number IN set_rp;
CREATE TABLE mt_rp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mt_rp VALUES (1, 10), (2, 20), (3, 30);

SET mutations_sync = 2;
SET alter_update_mode = 'heavy';

SELECT '-- without a policy the view is usable';
ALTER TABLE mt_rp UPDATE v = (SELECT max(number) FROM v_rp) WHERE k = 1;
SELECT * FROM mt_rp ORDER BY k;

CREATE ROW POLICY rp_set_rp ON set_rp USING k = 1 TO ALL;

SELECT '-- with a policy the assigned value is refused';
SELECT max(number) FROM v_rp; -- { serverError ACCESS_DENIED }
ALTER TABLE mt_rp UPDATE v = (SELECT max(number) FROM v_rp) WHERE k = 3; -- { serverError ACCESS_DENIED }
ALTER TABLE mt_rp UPDATE v = v + (SELECT max(number) FROM v_rp) WHERE k = 3; -- { serverError ACCESS_DENIED }
ALTER TABLE mt_rp UPDATE v = k IN (SELECT number FROM numbers(10) WHERE number = (SELECT max(number) FROM v_rp)) WHERE k = 3; -- { serverError ACCESS_DENIED }
SELECT * FROM mt_rp ORDER BY k;

SELECT '-- a subquery inside a lambda or next to an alias is still accepted';
ALTER TABLE mt_rp UPDATE v = arrayExists(x -> x IN (SELECT 3), [k]) WHERE k = 3;
ALTER TABLE mt_rp UPDATE v = ((k + 1) AS kk) * (kk IN (SELECT 3)) WHERE k = 2;
SELECT * FROM mt_rp ORDER BY k;

SELECT '-- a subquery may use an alias of its command, and two commands may use the same alias';
ALTER TABLE mt_rp UPDATE v = (5 AS c) + (SELECT c) WHERE k = 1;
ALTER TABLE mt_rp UPDATE v = (SELECT c) WHERE k = (2 AS c);
ALTER TABLE mt_rp UPDATE v = ((SELECT 7) AS x) WHERE k = 1, UPDATE v = ((SELECT 8) AS x) WHERE k = 3;
SELECT * FROM mt_rp ORDER BY k;

DROP ROW POLICY rp_set_rp ON set_rp;
DROP VIEW v_rp;
DROP TABLE set_rp;
DROP TABLE mt_rp;
