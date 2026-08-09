-- A `SELECT *` view freezes the wildcard expansion into its own column list at creation time, and
-- that expansion is not limited to the insertable columns of the target table: under
-- `asterisk_include_materialized_columns` / `asterisk_include_alias_columns` the view's header also
-- contains the target's `MATERIALIZED` and `ALIAS` columns. Such a view must be rejected as not
-- insertable up front, rather than failing deep inside the nested `INSERT` with a message naming a
-- column and a table that the user never wrote.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS v_mat;
DROP TABLE IF EXISTS v_alias;
DROP TABLE IF EXISTS v_plain;

CREATE TABLE t (a UInt32, b UInt32 DEFAULT 42, m UInt32 MATERIALIZED a + 1, al UInt32 ALIAS a + 2)
ENGINE = MergeTree ORDER BY tuple();

CREATE VIEW v_mat AS SELECT * FROM t SETTINGS asterisk_include_materialized_columns = 1;
CREATE VIEW v_alias AS SELECT * FROM t SETTINGS asterisk_include_alias_columns = 1;

SELECT 'materialized column in the view header';
INSERT INTO v_mat (a) VALUES (1); -- { serverError NOT_IMPLEMENTED }

SELECT 'alias column in the view header';
INSERT INTO v_alias (a) VALUES (1); -- { serverError NOT_IMPLEMENTED }

SELECT 'nothing was written';
SELECT count() FROM t;

SELECT 'a plain wildcard view is still insertable';
CREATE VIEW v_plain AS SELECT * FROM t;
INSERT INTO v_plain (a) VALUES (1);
SELECT a, b, m, al FROM t ORDER BY a;

DROP TABLE v_plain;
DROP TABLE v_alias;
DROP TABLE v_mat;
DROP TABLE t;
