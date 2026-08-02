-- Tests that view transparency of the modification hash is implemented at the storage level, so it is
-- visible through every carrier of the hash, not only through a direct query: `system.tables.modification_hash`
-- must be filled for a `View` and a `MaterializedView`, and a wrapper engine such as `Merge` over a view
-- must keep reporting a hash instead of failing closed.

DROP TABLE IF EXISTS t_04669;
CREATE TABLE t_04669 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04669 VALUES (1);

CREATE VIEW v_04669 AS SELECT x FROM t_04669;

DROP TABLE IF EXISTS mv_src_04669;
CREATE TABLE mv_src_04669 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_04669 ENGINE = MergeTree ORDER BY x AS SELECT x FROM mv_src_04669;
INSERT INTO mv_src_04669 VALUES (1);

-- A `Merge` table wrapping the view. It recurses into the view's hash, which is only possible when the view
-- reports one at the storage level.
CREATE TABLE merge_over_view_04669 (x UInt64) ENGINE = Merge(currentDatabase(), '^v_04669$');

CREATE TABLE hashes_04669 (step String, name String, hash Nullable(UInt128)) ENGINE = Memory;

INSERT INTO hashes_04669
    SELECT 'before', name, modification_hash FROM system.tables
    WHERE database = currentDatabase() AND name IN ('v_04669', 'mv_04669', 'merge_over_view_04669');

SELECT 'not null', name, hash IS NOT NULL FROM hashes_04669 WHERE step = 'before' ORDER BY name;

INSERT INTO t_04669 VALUES (2);
INSERT INTO mv_src_04669 VALUES (2);

INSERT INTO hashes_04669
    SELECT 'after', name, modification_hash FROM system.tables
    WHERE database = currentDatabase() AND name IN ('v_04669', 'mv_04669', 'merge_over_view_04669');

-- Every hash changed: the view, the materialized view, and the `Merge` over the view all follow the data
-- behind them.
SELECT 'changed', name, uniqExact(hash) = 2 FROM hashes_04669 GROUP BY name ORDER BY name;

-- Redefining the view is a modification even though the data behind it did not change.
DROP TABLE v_04669;
CREATE VIEW v_04669 AS SELECT x + 1 AS x FROM t_04669;
SELECT 'redefined', modification_hash != (SELECT hash FROM hashes_04669 WHERE step = 'after' AND name = 'v_04669')
FROM system.tables WHERE database = currentDatabase() AND name = 'v_04669';

-- Fail closed: a view over a table that cannot tell whether it changed reports no hash at all.
CREATE VIEW v_no_hash_04669 AS SELECT number FROM system.numbers LIMIT 1;
SELECT 'fail closed', modification_hash IS NULL FROM system.tables
WHERE database = currentDatabase() AND name = 'v_no_hash_04669';

DROP TABLE v_no_hash_04669;
DROP TABLE merge_over_view_04669;
DROP TABLE mv_04669;
DROP TABLE mv_src_04669;
DROP TABLE v_04669;
DROP TABLE t_04669;
DROP TABLE hashes_04669;
