DROP TABLE IF EXISTS t;
CREATE TABLE t(a Int32, b String, c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;

-- Simple view: insert is forwarded to the underlying table
DROP VIEW IF EXISTS v;
CREATE VIEW v AS SELECT a, b FROM t;
INSERT INTO v VALUES (1, 'hello');
SELECT * FROM t ORDER BY a;

-- Column c gets its default value 0.5
INSERT INTO v VALUES (2, 'world');
SELECT * FROM t ORDER BY a;

-- View with asterisk: all columns are insertable
DROP VIEW IF EXISTS v_asterisk;
CREATE VIEW v_asterisk AS SELECT * FROM t;
INSERT INTO v_asterisk VALUES (3, 'asterisk', 1.5);
SELECT * FROM t ORDER BY a;

-- View with aliases: use the alias names when inserting
DROP VIEW IF EXISTS v_alias;
CREATE VIEW v_alias AS SELECT a AS id, b AS name FROM t;
INSERT INTO v_alias VALUES (4, 'alias');
SELECT * FROM t ORDER BY a;

-- View with WHERE: acts as a constraint
DROP VIEW IF EXISTS v_positive;
CREATE VIEW v_positive AS SELECT a, b FROM t WHERE a > 0;
INSERT INTO v_positive VALUES (5, 'ok');     -- succeeds
SELECT * FROM t ORDER BY a;
INSERT INTO v_positive VALUES (-1, 'fail');  -- { serverError VIOLATED_CONSTRAINT }

-- ORDER BY is allowed and ignored for inserts
DROP VIEW IF EXISTS v_ordered;
CREATE VIEW v_ordered AS SELECT a, b FROM t ORDER BY a;
INSERT INTO v_ordered VALUES (6, 'fine');    -- succeeds
SELECT * FROM t ORDER BY a;

-- PREWHERE is rejected
DROP VIEW IF EXISTS v_prewhere;
CREATE VIEW v_prewhere AS SELECT a, b FROM t PREWHERE a > 0;
INSERT INTO v_prewhere VALUES (6, 'prewhere'); -- { serverError NOT_IMPLEMENTED }

-- WHERE with lambda is allowed
DROP VIEW IF EXISTS v_lambda;
CREATE VIEW v_lambda AS SELECT a, b FROM t WHERE arrayExists(x -> x > 0, [a]);
INSERT INTO v_lambda VALUES (7, 'lambda');  -- succeeds since a=7 >0
SELECT * FROM t ORDER BY a;

-- Asterisk with expressions is rejected (cannot mix * with explicit columns)
DROP VIEW IF EXISTS v_mixed;
CREATE VIEW v_mixed AS SELECT *, a + 1 AS derived FROM t;
INSERT INTO v_mixed VALUES (8, 'mixed', 0.1, 9); -- { serverError NOT_IMPLEMENTED }

-- Asterisk with simple column is rejected (cannot mix)
DROP VIEW IF EXISTS v_mixed2;
CREATE VIEW v_mixed2 AS SELECT *, b AS extra FROM t;
INSERT INTO v_mixed2 VALUES (9, 'mixed2', 0.2, 'extra'); -- { serverError NOT_IMPLEMENTED }

-- Asterisk with column transformers is rejected
DROP VIEW IF EXISTS v_replace;
CREATE VIEW v_replace AS SELECT * REPLACE(a + 1 AS a) FROM t;
INSERT INTO v_replace VALUES (10, 'replace', 1.0); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t;
