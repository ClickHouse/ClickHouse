-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116090
-- A correlated subquery over a `Distributed` table is refused with a declared error. Through a `VIEW`
-- the check did not fire: a view is an opaque `TABLE` node at validation time - the analyzer expands
-- it later - so its own `isRemote` is false. Planning then proceeded and broke a planner invariant
-- with `Column identifier ... is already registered`, a `LOGICAL_ERROR` that aborts a debug build.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_corr_view_local;
DROP TABLE IF EXISTS t_corr_view_dist;
DROP TABLE IF EXISTS t_corr_view_mv_target;
DROP TABLE IF EXISTS m_corr_view_merge;
DROP TABLE IF EXISTS m_corr_view_merge_local;
DROP VIEW IF EXISTS v_corr_view_remote;
DROP VIEW IF EXISTS v_corr_view_local;
DROP VIEW IF EXISTS v_corr_view_nested;
DROP VIEW IF EXISTS pv_corr_view_remote;
DROP VIEW IF EXISTS mv_corr_view_remote_source;
DROP VIEW IF EXISTS mv_corr_view_remote_target;
DROP VIEW IF EXISTS mv_corr_view_dist_target;

CREATE TABLE t_corr_view_local (n UInt32, k UInt32, v Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_corr_view_local SELECT number % 10, number, number * 10 FROM numbers(100);
CREATE TABLE t_corr_view_dist AS t_corr_view_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_corr_view_local);
CREATE VIEW v_corr_view_remote AS SELECT * FROM t_corr_view_dist;
CREATE VIEW v_corr_view_local AS SELECT * FROM t_corr_view_local;
CREATE VIEW v_corr_view_nested AS SELECT * FROM v_corr_view_remote;

SELECT 'directly over the Distributed table';
SELECT o.v FROM t_corr_view_dist AS o WHERE EXISTS (SELECT 1 FROM t_corr_view_dist AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

SELECT 'through a view';
SELECT o.v FROM v_corr_view_remote AS o WHERE EXISTS (SELECT 1 FROM v_corr_view_remote AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }
SELECT o.n FROM v_corr_view_remote AS o WHERE o.v = (SELECT max(i.v) FROM v_corr_view_remote AS i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT o.n FROM t_corr_view_local AS o WHERE EXISTS (SELECT 1 FROM v_corr_view_remote AS i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }

SELECT 'through a view over a view';
SELECT o.v FROM v_corr_view_nested AS o WHERE EXISTS (SELECT 1 FROM v_corr_view_nested AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

-- A parameterized view is resolved as a `TableFunctionNode` wrapping the real `StorageView`, not as a
-- `TableNode`, so it needs the same look-through.
SELECT 'through a parameterized view';
CREATE VIEW pv_corr_view_remote AS SELECT * FROM t_corr_view_dist WHERE n = {pn:UInt32};
SELECT o.v FROM pv_corr_view_remote(pn = 1) AS o WHERE EXISTS (SELECT 1 FROM pv_corr_view_remote(pn = 1) AS i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }

-- `Merge` matches its sources by a pattern resolved at read time, so the catalog records no
-- referential dependency for it and its own `isRemote` asks every source only about itself.
SELECT 'through a Merge over the view';
CREATE TABLE m_corr_view_merge AS t_corr_view_local ENGINE = Merge(currentDatabase(), '^v_corr_view_remote$');
SELECT o.v FROM m_corr_view_merge AS o WHERE EXISTS (SELECT 1 FROM m_corr_view_merge AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

-- Reading a materialized view reads its target table, so the target needs the same look-through as any
-- other table: an ordinary `VIEW` is accepted as a `TO` target - the create-time check only samples the
-- target's insertable columns - and `StorageMaterializedView::isRemote` asks the target only about itself.
SELECT 'through a materialized view whose target is a view over the Distributed table';
CREATE MATERIALIZED VIEW mv_corr_view_remote_target TO v_corr_view_remote AS SELECT * FROM t_corr_view_local;
SELECT o.v FROM mv_corr_view_remote_target AS o WHERE EXISTS (SELECT 1 FROM mv_corr_view_remote_target AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

SELECT 'through a materialized view whose target is the Distributed table';
CREATE MATERIALIZED VIEW mv_corr_view_dist_target TO t_corr_view_dist AS SELECT * FROM t_corr_view_local;
SELECT o.v FROM mv_corr_view_dist_target AS o WHERE EXISTS (SELECT 1 FROM mv_corr_view_dist_target AS i WHERE i.n = o.n); -- { serverError NOT_IMPLEMENTED }

SELECT 'a view over a local table still works';
SELECT count() FROM (SELECT o.v FROM v_corr_view_local AS o WHERE EXISTS (SELECT 1 FROM v_corr_view_local AS i WHERE i.n = o.n));
SELECT count() FROM (SELECT o.v FROM t_corr_view_local AS o WHERE EXISTS (SELECT 1 FROM t_corr_view_local AS i WHERE i.n = o.n));

SELECT 'a Merge over local tables still works';
CREATE TABLE m_corr_view_merge_local AS t_corr_view_local ENGINE = Merge(currentDatabase(), '^t_corr_view_local$');
SELECT count() FROM (SELECT o.v FROM m_corr_view_merge_local AS o WHERE EXISTS (SELECT 1 FROM m_corr_view_merge_local AS i WHERE i.n = o.n));

-- Reading a materialized view only reads its target table; the `Distributed` source is reachable at
-- insert time only, so following the catalog dependencies blindly would refuse a working query.
SELECT 'a materialized view with a remote source and a local target still works';
CREATE TABLE t_corr_view_mv_target (n UInt32, k UInt32, v Int64) ENGINE = MergeTree ORDER BY n;
CREATE MATERIALIZED VIEW mv_corr_view_remote_source TO t_corr_view_mv_target AS SELECT * FROM t_corr_view_dist;
SELECT count() FROM (SELECT o.v FROM mv_corr_view_remote_source AS o WHERE EXISTS (SELECT 1 FROM mv_corr_view_remote_source AS i WHERE i.n = o.n));

SELECT 'an uncorrelated subquery over the view still works';
SELECT count() FROM (SELECT o.v FROM v_corr_view_remote AS o WHERE o.n IN (SELECT n FROM v_corr_view_remote));

DROP VIEW mv_corr_view_dist_target;
DROP VIEW mv_corr_view_remote_target;
DROP VIEW mv_corr_view_remote_source;
DROP TABLE t_corr_view_mv_target;
DROP TABLE m_corr_view_merge_local;
DROP TABLE m_corr_view_merge;
DROP VIEW pv_corr_view_remote;
DROP VIEW v_corr_view_nested;
DROP VIEW v_corr_view_remote;
DROP VIEW v_corr_view_local;
DROP TABLE t_corr_view_dist;
DROP TABLE t_corr_view_local;
