-- Reading through `merge()` over a child table whose ALIAS column contains `IN` used to abort with
-- `Logical error: No set is registered for key ...`. It triggers only when the children disagree
-- about that column's default, which makes the Merge-level column look physical.

-- The old analyzer reads an ALIAS column through `TreeRewriter`, which never consults the set
-- registry, so every case below produces its expected value on an unfixed server unless this is set.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t04812_phys;
DROP TABLE IF EXISTS t04812_alias;
DROP TABLE IF EXISTS t04812_set;

-- Constant tuple: the `findTuple` branch.
CREATE TABLE t04812_phys (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t04812_alias (y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_phys VALUES (7);
INSERT INTO t04812_alias VALUES (2), (9);
SELECT 'constant tuple';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^t04812_(phys|alias)$');

-- The same shape with an explicit column list, not `SELECT *`.
SELECT 'explicit column';
SELECT arraySort(groupArray(x)) FROM (SELECT x FROM merge(currentDatabase(), '^t04812_(phys|alias)$'));

-- A filter reaching the child read, which is how the failure was first seen in CI.
SELECT 'with filter';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^t04812_(phys|alias)$') WHERE x = 1;

-- `Set` table: the `findStorage` branch, whose registry key carries no element types.
DROP TABLE t04812_alias;
CREATE TABLE t04812_set (k UInt8) ENGINE = Set;
INSERT INTO t04812_set VALUES (1), (2);
CREATE TABLE t04812_alias (y UInt8, x UInt8 ALIAS y IN t04812_set) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_alias VALUES (2), (9);
SELECT 'set table';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^t04812_(phys|alias)$');

-- `NOT IN` reaches the same lookup.
DROP TABLE t04812_alias;
CREATE TABLE t04812_alias (y UInt8, x UInt8 ALIAS y NOT IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_alias VALUES (2), (9);
SELECT 'not in';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^t04812_(phys|alias)$');

-- A tuple key, so the registry key carries two element types.
DROP TABLE t04812_alias;
CREATE TABLE t04812_alias (a UInt8, b UInt8, x UInt8 ALIAS (a, b) IN ((1, 2), (3, 4))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_alias VALUES (1, 2), (9, 9);
SELECT 'tuple key';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^t04812_(phys|alias)$');

-- A Nullable operand, where the alias itself is Nullable.
DROP TABLE t04812_phys;
DROP TABLE t04812_alias;
CREATE TABLE t04812_phys (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t04812_alias (y Nullable(UInt8), x Nullable(UInt8) ALIAS y IN (1, 2, NULL)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t04812_phys VALUES (7);
INSERT INTO t04812_alias VALUES (2), (NULL);
SELECT 'nullable';
SELECT arraySort(groupArray(ifNull(x, 255))), countIf(x IS NULL) FROM merge(currentDatabase(), '^t04812_(phys|alias)$');

DROP TABLE t04812_phys;
DROP TABLE t04812_alias;
DROP TABLE t04812_set;

-- A third child identical to the second takes the query info cache path, which skips
-- `getModifiedQueryInfo` and evaluates its alias in `convertAndFilterSourceStream` instead.
-- The values are asserted so that a silently unevaluated alias fails this case.
DROP TABLE IF EXISTS u04812_phys;
DROP TABLE IF EXISTS u04812_a;
DROP TABLE IF EXISTS u04812_b;
CREATE TABLE u04812_phys (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE u04812_a (y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE u04812_b (y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO u04812_phys VALUES (7);
INSERT INTO u04812_a VALUES (2);
INSERT INTO u04812_b VALUES (9);
SELECT 'query info cache';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^u04812_');

DROP TABLE u04812_phys;
DROP TABLE u04812_a;
DROP TABLE u04812_b;

-- Control: with both children declaring the same ALIAS there is no disagreement, the Merge column
-- keeps its ALIAS, and this case already passed before the fix. It is what shows that the
-- discriminating condition of the cases above is the disagreement between children.
DROP TABLE IF EXISTS v04812_a;
DROP TABLE IF EXISTS v04812_b;
CREATE TABLE v04812_a (y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE v04812_b (y UInt8, x UInt8 ALIAS y IN (1, 2, 3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO v04812_a VALUES (2);
INSERT INTO v04812_b VALUES (9);
SELECT 'identical children';
SELECT arraySort(groupArray(x)) FROM merge(currentDatabase(), '^v04812_');
SELECT default_kind, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'v04812_a' AND name = 'x';

DROP TABLE v04812_a;
DROP TABLE v04812_b;
