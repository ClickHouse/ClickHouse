-- The column set of `CREATE TABLE ... AS` must not depend on `use_legacy_to_time`: re-deriving
-- columns after the legacy `toTime` rewrite flattened `Nested` columns that the `AS SELECT` and
-- `AS src` branches keep intact.

DROP TABLE IF EXISTS t_nested_legacy;
DROP TABLE IF EXISTS t_nested_legacy_dst;

SET flatten_nested = 1;
SET use_legacy_to_time = 1;

CREATE TABLE t_nested_legacy ENGINE = Memory AS SELECT [(1, 'a')]::Nested(a UInt8, b String) AS n;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_nested_legacy' ORDER BY name;
SELECT * FROM t_nested_legacy;

-- A plain copy of the `Nested`-typed source must stay intact under the legacy setting too.
CREATE TABLE t_nested_legacy_dst AS t_nested_legacy;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_nested_legacy_dst' ORDER BY name;

DROP TABLE t_nested_legacy;
DROP TABLE t_nested_legacy_dst;
