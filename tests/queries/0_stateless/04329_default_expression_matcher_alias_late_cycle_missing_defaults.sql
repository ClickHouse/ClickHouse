-- Matcher expansion in stored expressions is settings-independent, so a cycle created by
-- adding an ALIAS/MATERIALIZED column matched by an existing `COLUMNS` default is detected
-- eagerly at ALTER time instead of surfacing later at INSERT time. Columns matched only
-- through `*` are never ALIAS/MATERIALIZED, so the equivalent `*` ALTER succeeds and
-- inserts stay deterministic regardless of `asterisk_include_*` settings.
DROP TABLE IF EXISTS default_expression_matcher_cycle_alter_direct;
DROP TABLE IF EXISTS default_expression_matcher_cycle_alter_indirect;
DROP TABLE IF EXISTS default_expression_matcher_asterisk_no_cycle;

CREATE TABLE default_expression_matcher_cycle_alter_direct
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(COLUMNS('^(a|m|x)$')))
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_cycle_alter_direct ADD COLUMN m String MATERIALIZED b; -- { serverError CYCLIC_ALIASES }
ALTER TABLE default_expression_matcher_cycle_alter_direct ADD COLUMN x String ALIAS b; -- { serverError CYCLIC_ALIASES }

INSERT INTO default_expression_matcher_cycle_alter_direct (a) VALUES (1);
SELECT b FROM default_expression_matcher_cycle_alter_direct;

CREATE TABLE default_expression_matcher_cycle_alter_indirect
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(COLUMNS('^(a|x)$')))
)
ENGINE = Memory;

-- `y` is not matched by the pattern, so this is acyclic and allowed.
ALTER TABLE default_expression_matcher_cycle_alter_indirect ADD COLUMN y String ALIAS b;
-- `x` is matched by the pattern and closes the `b` -> `x` -> `y` -> `b` cycle.
ALTER TABLE default_expression_matcher_cycle_alter_indirect ADD COLUMN x String ALIAS y; -- { serverError CYCLIC_ALIASES }

INSERT INTO default_expression_matcher_cycle_alter_indirect (a) VALUES (1);
SELECT b FROM default_expression_matcher_cycle_alter_indirect;

CREATE TABLE default_expression_matcher_asterisk_no_cycle
(
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b))
)
ENGINE = Memory;

ALTER TABLE default_expression_matcher_asterisk_no_cycle ADD COLUMN m String MATERIALIZED b;
ALTER TABLE default_expression_matcher_asterisk_no_cycle ADD COLUMN x String ALIAS b;

SET asterisk_include_alias_columns = 1;
SET asterisk_include_materialized_columns = 1;

-- `*` never expands to ALIAS/MATERIALIZED columns in stored expressions, so no cycle forms
-- and the stored value does not depend on the session settings.
INSERT INTO default_expression_matcher_asterisk_no_cycle (a) VALUES (1);
SELECT b, m, x FROM default_expression_matcher_asterisk_no_cycle;

DROP TABLE default_expression_matcher_asterisk_no_cycle;
DROP TABLE default_expression_matcher_cycle_alter_indirect;
DROP TABLE default_expression_matcher_cycle_alter_direct;
