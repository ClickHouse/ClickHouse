-- A default expression that reads another column through a subcolumn path (`y.x`) does not close a
-- `DEFAULT` cycle at DDL time: the table is created, and the dependency is resolved per insert from
-- the columns that are actually supplied. This mirrors `04040_defaults_dependency_order`, which pins
-- the same behaviour for a three-column cycle of tuple subcolumn reads on `master`. Only cycles
-- between whole-column reads are rejected up front.

SELECT '-- CREATE: reference cycle through a tuple subcolumn is accepted';
DROP TABLE IF EXISTS t_default_cycle_subcolumn;
CREATE TABLE t_default_cycle_subcolumn
(
    a UInt8 DEFAULT y.x,
    y Tuple(x UInt8) DEFAULT tuple(a)
) ENGINE = MergeTree ORDER BY tuple();

-- Supplying either column resolves the other one.
INSERT INTO t_default_cycle_subcolumn (a) VALUES (7);
INSERT INTO t_default_cycle_subcolumn (y) VALUES (tuple(9));
SELECT a, y FROM t_default_cycle_subcolumn ORDER BY a;
DROP TABLE t_default_cycle_subcolumn;

SELECT '-- CREATE: cycle between whole-column reads is still rejected';
DROP TABLE IF EXISTS t_default_cycle_whole_column;
CREATE TABLE t_default_cycle_whole_column
(
    a UInt8 DEFAULT y + 1,
    y UInt8 DEFAULT a + 1
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError CYCLIC_ALIASES }

SELECT '-- ALTER: reference cycle through a tuple subcolumn is accepted';
DROP TABLE IF EXISTS t_alter_cycle_subcolumn;
CREATE TABLE t_alter_cycle_subcolumn
(
    a UInt8,
    y Tuple(x UInt8) DEFAULT tuple(a)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN a UInt8 DEFAULT y.x;

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_cycle_subcolumn' AND name = 'a';

SELECT '-- acyclic subcolumn reads are still accepted';
-- Reading `y.x` is fine as long as `y` does not read back from `a`.
ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN y Tuple(x UInt8) DEFAULT tuple(toUInt8(0));
ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN a UInt8 DEFAULT y.x + 1;
INSERT INTO t_alter_cycle_subcolumn (y) VALUES (tuple(41));
SELECT a, y FROM t_alter_cycle_subcolumn;

DROP TABLE t_alter_cycle_subcolumn;
