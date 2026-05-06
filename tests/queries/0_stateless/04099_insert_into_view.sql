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

-- View with aliases: use the alias names when inserting
DROP VIEW IF EXISTS v_alias;
CREATE VIEW v_alias AS SELECT a AS id, b AS name FROM t;
INSERT INTO v_alias VALUES (3, 'alias');
SELECT * FROM t ORDER BY a;

-- View with WHERE: acts as a constraint
DROP VIEW IF EXISTS v_positive;
CREATE VIEW v_positive AS SELECT a, b FROM t WHERE a > 0;
INSERT INTO v_positive VALUES (4, 'ok');     -- succeeds
SELECT * FROM t ORDER BY a;
INSERT INTO v_positive VALUES (-1, 'fail');  -- fails with VIOLATED_CONSTRAINT

-- ORDER BY is allowed and ignored for inserts
DROP VIEW IF EXISTS v_ordered;
CREATE VIEW v_ordered AS SELECT a, b FROM t ORDER BY a;
INSERT INTO v_ordered VALUES (5, 'fine');    -- succeeds
SELECT * FROM t ORDER BY a;

-- PREWHERE is rejected
DROP VIEW IF EXISTS v_prewhere;
CREATE VIEW v_prewhere AS SELECT a, b FROM t PREWHERE a > 0; -- { serverError NOT_IMPLEMENTED }

-- WHERE with lambda is allowed
DROP VIEW IF EXISTS v_lambda;
CREATE VIEW v_lambda AS SELECT a, b FROM t WHERE arrayExists(x -> x > 0, [a]);
INSERT INTO v_lambda VALUES (6, 'lambda');  -- succeeds since a=6 >0
SELECT * FROM t ORDER BY a;

-- WHERE referencing unprojected column is rejected
DROP VIEW IF EXISTS v_unprojected;
CREATE VIEW v_unprojected AS SELECT a FROM t WHERE b = 'test'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t;