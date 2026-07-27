-- A default expression that reads another column through a subcolumn path (`y.x`) depends on
-- the whole storage column `y`, so such an edge must participate in the `DEFAULT` cycle graph.
-- Otherwise a real cycle `a -> y -> a` slips through the DDL validation and only explodes later.

SELECT '-- CREATE: cycle through a tuple subcolumn';
DROP TABLE IF EXISTS t_default_cycle_subcolumn;
CREATE TABLE t_default_cycle_subcolumn
(
    a UInt8 DEFAULT y.x,
    y Tuple(x UInt8) DEFAULT tuple(a)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError CYCLIC_ALIASES }

SELECT '-- ALTER: cycle through a tuple subcolumn';
DROP TABLE IF EXISTS t_alter_cycle_subcolumn;
CREATE TABLE t_alter_cycle_subcolumn
(
    a UInt8,
    y Tuple(x UInt8) DEFAULT tuple(a)
) ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN a UInt8 DEFAULT y.x; -- { serverError CYCLIC_ALIASES }

-- The rejected `ALTER` did not change the metadata.
SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_cycle_subcolumn' AND name = 'a';

SELECT '-- acyclic subcolumn reads are still accepted';
-- Reading `y.x` is fine as long as `y` does not read back from `a`.
ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN y Tuple(x UInt8) DEFAULT tuple(toUInt8(0));
ALTER TABLE t_alter_cycle_subcolumn MODIFY COLUMN a UInt8 DEFAULT y.x + 1;
INSERT INTO t_alter_cycle_subcolumn (y) VALUES (tuple(41));
SELECT a, y FROM t_alter_cycle_subcolumn;

DROP TABLE t_alter_cycle_subcolumn;
