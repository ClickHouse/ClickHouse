-- Statistics declared on a `Nested` column are flattened together with the column: they are
-- retargeted at the physical `Array(...)` subcolumn type, so materializing them does not report a
-- type mismatch as a logical error. `auto_statistics_types = ''` keeps the implicit statistics out
-- of the way, because merging them in also refreshes the type.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_nested_statistics;

-- Declared in `CREATE TABLE`.
CREATE TABLE t_nested_statistics (a UInt64, n Nested(x UInt64) STATISTICS(basic))
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';

SELECT name, type, statistics FROM system.columns
WHERE database = currentDatabase() AND table = 't_nested_statistics' ORDER BY name;

INSERT INTO t_nested_statistics VALUES (1, [2, 3]);
SELECT count() FROM t_nested_statistics;

DROP TABLE t_nested_statistics;

-- Declared in `ALTER TABLE ... ADD COLUMN`.
CREATE TABLE t_nested_statistics (a UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';

ALTER TABLE t_nested_statistics ADD COLUMN n Nested(x UInt64) STATISTICS(basic);

SELECT name, type, statistics FROM system.columns
WHERE database = currentDatabase() AND table = 't_nested_statistics' ORDER BY name;

INSERT INTO t_nested_statistics VALUES (1, [2, 3]);
SELECT count() FROM t_nested_statistics;

-- A statistics type that the flattened `Array(...)` type does not support is rejected.
ALTER TABLE t_nested_statistics ADD COLUMN m Nested(y UInt64) STATISTICS(tdigest); -- { serverError ILLEGAL_STATISTICS }

DROP TABLE t_nested_statistics;

CREATE TABLE t_nested_statistics (a UInt64, m Nested(y UInt64) STATISTICS(tdigest))
ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = ''; -- { serverError ILLEGAL_STATISTICS }
