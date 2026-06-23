-- Regression test for emitting a non-first, non-key column from a `StorageJoin` (`Join` engine)
-- table. The compact row-ref emit path resolves output columns through `StoredColumnsIndex` against
-- the shared stored blocks; this checks that a query selecting only a later column (or a reordered
-- subset) reads the correct column - on the singleton (`ANY`), the duplicate-key (`ALL` -> RowRefList)
-- and the `joinGet` paths. The non-key columns have distinct types so a wrong-column read fails
-- loudly (wrong value or a type mismatch) rather than silently returning plausible data.

DROP TABLE IF EXISTS t_join_any;
DROP TABLE IF EXISTS t_join_all;

CREATE TABLE t_join_any (k UInt32, a UInt64, b String, c Int32) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_join_any VALUES (1, 1100, 'bee1', -11), (2, 2200, 'bee2', -22), (3, 3300, 'bee3', -33);

-- MapsOne (ANY) emit. Select only the 2nd non-key column, only the 3rd, then a reordered subset.
-- Key 4 has no match, so the LEFT-join default path is exercised too.
SELECT 'any_b', l.k, j.b FROM (SELECT toUInt32(arrayJoin([1, 2, 3, 4])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;
SELECT 'any_c', l.k, j.c FROM (SELECT toUInt32(arrayJoin([1, 2, 3, 4])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;
SELECT 'any_cb', l.k, j.c, j.b FROM (SELECT toUInt32(arrayJoin([1, 2, 3])) AS k) AS l ANY LEFT JOIN t_join_any AS j ON l.k = j.k ORDER BY l.k;

-- joinGet of non-first columns.
SELECT 'joinget_b', joinGet(currentDatabase() || '.t_join_any', 'b', toUInt32(2));
SELECT 'joinget_c', joinGet(currentDatabase() || '.t_join_any', 'c', toUInt32(3));

CREATE TABLE t_join_all (k UInt32, a UInt64, b String) ENGINE = Join(ALL, INNER, k);
INSERT INTO t_join_all VALUES (1, 10, 'x1'), (1, 20, 'x2'), (2, 30, 'y1');

-- MapsAll (ALL, duplicate keys -> RowRefList) emit. Select only the non-first non-key column.
SELECT 'all_b', l.k, j.b FROM (SELECT toUInt32(arrayJoin([1, 2])) AS k) AS l ALL INNER JOIN t_join_all AS j ON l.k = j.k ORDER BY l.k, j.b;

DROP TABLE t_join_any;
DROP TABLE t_join_all;
