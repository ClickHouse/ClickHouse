-- Test direct SELECT reads of RIGHT-kind Join engine tables with ANY/SEMI/ANTI strictness,
-- which dispatch to fillAll and must emit every stored row (including duplicate keys).
-- Each table is populated with one row per INSERT so StorageJoin::insertBlock forwards each
-- to a separate HashJoin stored block (fresh block_no per call), forcing the direct read to
-- resolve refs across more than one {block_no, row_no}.
DROP TABLE IF EXISTS jr_any;
DROP TABLE IF EXISTS jr_semi;
DROP TABLE IF EXISTS jr_anti;

CREATE TABLE jr_any (k UInt32, v String) ENGINE = Join(ANY, RIGHT, k);
INSERT INTO jr_any VALUES (1, 'a');
INSERT INTO jr_any VALUES (1, 'b');
INSERT INTO jr_any VALUES (2, 'c');
SELECT 'ANY RIGHT';
SELECT k, v FROM jr_any ORDER BY k, v;

CREATE TABLE jr_semi (k UInt32, v String) ENGINE = Join(SEMI, RIGHT, k);
INSERT INTO jr_semi VALUES (1, 'a');
INSERT INTO jr_semi VALUES (1, 'b');
INSERT INTO jr_semi VALUES (2, 'c');
SELECT 'SEMI RIGHT';
SELECT k, v FROM jr_semi ORDER BY k, v;

CREATE TABLE jr_anti (k UInt32, v String) ENGINE = Join(ANTI, RIGHT, k);
INSERT INTO jr_anti VALUES (1, 'a');
INSERT INTO jr_anti VALUES (1, 'b');
INSERT INTO jr_anti VALUES (2, 'c');
SELECT 'ANTI RIGHT';
SELECT k, v FROM jr_anti ORDER BY k, v;

DROP TABLE jr_any;
DROP TABLE jr_semi;
DROP TABLE jr_anti;
