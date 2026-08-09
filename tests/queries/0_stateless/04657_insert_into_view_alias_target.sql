-- Pins the handling of an `Alias` table as the target of an insertable normal view (issue #91535).
-- An `Alias` forwards both its metadata and its writes to its own target, so the "the target of an
-- insertable view must not be a view" rule has to be checked against the storage the alias
-- ultimately resolves to, not against the alias itself.

SET allow_experimental_alias_table_engine = 1;

DROP TABLE IF EXISTS t_insert_into_view_alias_target;
DROP VIEW IF EXISTS v_insert_into_view_alias_target;
DROP VIEW IF EXISTS v_over_alias_insert_into_view_alias_target;
DROP TABLE IF EXISTS a_insert_into_view_alias_target;
DROP TABLE IF EXISTS a2_insert_into_view_alias_target;
DROP VIEW IF EXISTS v_over_alias_to_view_insert_into_view_alias_target;

CREATE TABLE t_insert_into_view_alias_target (a UInt8, b UInt8 DEFAULT 42) ENGINE = MergeTree ORDER BY tuple();

-- An alias to a real table is a valid target: the omitted column receives the target's DEFAULT.
CREATE TABLE a_insert_into_view_alias_target ENGINE = Alias('t_insert_into_view_alias_target');
CREATE VIEW v_over_alias_insert_into_view_alias_target AS SELECT a, b FROM a_insert_into_view_alias_target;

INSERT INTO v_over_alias_insert_into_view_alias_target (a) VALUES (1);
SELECT 'alias-to-table:', a, b FROM t_insert_into_view_alias_target ORDER BY a;

-- An alias to a view is not: the intermediate view carries no column DEFAULT, so an omitted column
-- would be stored as a type default instead of the target's DEFAULT.
CREATE VIEW v_insert_into_view_alias_target AS SELECT a, b FROM t_insert_into_view_alias_target;
CREATE TABLE a2_insert_into_view_alias_target ENGINE = Alias('v_insert_into_view_alias_target');
CREATE VIEW v_over_alias_to_view_insert_into_view_alias_target AS SELECT a, b FROM a2_insert_into_view_alias_target;

INSERT INTO v_over_alias_to_view_insert_into_view_alias_target (a) VALUES (2); -- { serverError NOT_IMPLEMENTED }

SELECT 'rejected-nothing-stored:', count() FROM t_insert_into_view_alias_target;

DROP VIEW v_over_alias_to_view_insert_into_view_alias_target;
DROP TABLE a2_insert_into_view_alias_target;
DROP VIEW v_insert_into_view_alias_target;
DROP VIEW v_over_alias_insert_into_view_alias_target;
DROP TABLE a_insert_into_view_alias_target;
DROP TABLE t_insert_into_view_alias_target;
