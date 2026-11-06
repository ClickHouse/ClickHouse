-- Tags: no-old-analyzer
-- no-old-analyzer: the guard lives in the planner, so the old analyzer returns UNKNOWN_TABLE here.

DROP TABLE IF EXISTS loop_no_columns_target;
DROP TABLE IF EXISTS loop_no_columns_alias;
DROP TABLE IF EXISTS loop_no_columns_base;
DROP VIEW IF EXISTS loop_no_columns_parameterized;
DROP TABLE IF EXISTS loop_no_columns_alias_to_view;

CREATE TABLE loop_no_columns_target (c0 Int32) ENGINE = Memory;
CREATE TABLE loop_no_columns_alias ENGINE = Alias(currentDatabase(), loop_no_columns_target);
DROP TABLE loop_no_columns_target;

SELECT '-- loop() over an alias whose target was dropped';
SELECT count() FROM loop(currentDatabase(), 'loop_no_columns_alias'); -- { serverError UNSUPPORTED_METHOD }
SELECT 1 FROM loop(currentDatabase(), 'loop_no_columns_alias'); -- { serverError UNSUPPORTED_METHOD }

SELECT '-- reading the dangling alias directly still names the missing target';
SELECT count() FROM loop_no_columns_alias; -- { serverError UNKNOWN_TABLE }
DESCRIBE TABLE loop_no_columns_alias; -- { serverError UNKNOWN_TABLE }

SELECT '-- the dangling alias remains enumerable in the system tables';
SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name = 'loop_no_columns_alias';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'loop_no_columns_alias';

SELECT '-- structure inference over the dangling alias stays empty and does not throw';
DESCRIBE TABLE loop(currentDatabase(), 'loop_no_columns_alias');

SELECT '-- a parameterized view has no columns by design, with and without an alias';
CREATE TABLE loop_no_columns_base (a Int32) ENGINE = Memory;
CREATE VIEW loop_no_columns_parameterized AS SELECT * FROM loop_no_columns_base WHERE a = {p:Int32};
SELECT count() FROM loop(currentDatabase(), 'loop_no_columns_parameterized'); -- { serverError UNSUPPORTED_METHOD }
CREATE TABLE loop_no_columns_alias_to_view ENGINE = Alias(currentDatabase(), loop_no_columns_parameterized);
SELECT count() FROM loop(currentDatabase(), 'loop_no_columns_alias_to_view'); -- { serverError UNSUPPORTED_METHOD }
DESCRIBE TABLE loop(currentDatabase(), 'loop_no_columns_parameterized');

SELECT '-- a healthy alias keeps working';
CREATE TABLE loop_no_columns_target (c0 Int32) ENGINE = Memory;
INSERT INTO loop_no_columns_target VALUES (1), (2), (3);
SELECT 1 FROM loop(currentDatabase(), 'loop_no_columns_alias') LIMIT 4;
DESCRIBE TABLE loop(currentDatabase(), 'loop_no_columns_alias');

DROP TABLE loop_no_columns_alias_to_view;
DROP VIEW loop_no_columns_parameterized;
DROP TABLE loop_no_columns_base;
DROP TABLE loop_no_columns_alias;
DROP TABLE loop_no_columns_target;
