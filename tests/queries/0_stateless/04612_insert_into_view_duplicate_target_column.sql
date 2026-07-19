-- Two view columns must not resolve to the same underlying table column: there is no clear
-- write semantics for such a view, so the INSERT is rejected up front with NOT_IMPLEMENTED
-- instead of surfacing a late DUPLICATE_COLUMN error from the nested insert the user did not write.

DROP TABLE IF EXISTS t_dup;
DROP VIEW IF EXISTS v_dup_alias;
DROP VIEW IF EXISTS v_dup_mixed;
DROP VIEW IF EXISTS v_no_dup;

CREATE TABLE t_dup (a Int32, b String) ENGINE = MergeTree ORDER BY a;

-- Both aliases resolve to the same target column `a`.
CREATE VIEW v_dup_alias AS SELECT a AS x, a AS y FROM t_dup;
INSERT INTO v_dup_alias VALUES (1, 2); -- { serverError NOT_IMPLEMENTED }
INSERT INTO v_dup_alias (x) VALUES (1); -- { serverError NOT_IMPLEMENTED }

-- A plain reference and an alias to the same target column.
CREATE VIEW v_dup_mixed AS SELECT a, a AS y FROM t_dup;
INSERT INTO v_dup_mixed VALUES (1, 2); -- { serverError NOT_IMPLEMENTED }

-- The same column referenced twice without any alias (`SELECT a, b, a`) cannot form a view at
-- all: view creation rejects duplicate column names with ILLEGAL_COLUMN, so only aliased
-- duplicates can reach the INSERT-time guard.

-- No duplicates: distinct target columns keep working, including with renames.
CREATE VIEW v_no_dup AS SELECT a AS x, b AS y FROM t_dup;
INSERT INTO v_no_dup VALUES (1, 'one');
SELECT 'no-dup:', a, b FROM t_dup ORDER BY a;

DROP VIEW v_no_dup;
DROP VIEW v_dup_mixed;
DROP VIEW v_dup_alias;
DROP TABLE t_dup;
