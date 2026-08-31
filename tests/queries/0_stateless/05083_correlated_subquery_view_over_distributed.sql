-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116090
-- A correlated subquery over a `Distributed` table is refused with a declared error. Through a `VIEW`
-- the check did not fire: a view is an opaque `TABLE` node at validation time - the analyzer expands
-- it later - so its own `isRemote` is false. Planning then proceeded and broke a planner invariant
-- with `Column identifier ... is already registered`, a `LOGICAL_ERROR` that aborts a debug build.

DROP TABLE IF EXISTS t_corr_view_local;
DROP TABLE IF EXISTS t_corr_view_dist;
DROP VIEW IF EXISTS v_corr_view_remote;
DROP VIEW IF EXISTS v_corr_view_local;
DROP VIEW IF EXISTS v_corr_view_nested;

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

SELECT 'a view over a local table still works';
SELECT count() FROM (SELECT o.v FROM v_corr_view_local AS o WHERE EXISTS (SELECT 1 FROM v_corr_view_local AS i WHERE i.n = o.n));
SELECT count() FROM (SELECT o.v FROM t_corr_view_local AS o WHERE EXISTS (SELECT 1 FROM t_corr_view_local AS i WHERE i.n = o.n));

SELECT 'an uncorrelated subquery over the view still works';
SELECT count() FROM (SELECT o.v FROM v_corr_view_remote AS o WHERE o.n IN (SELECT n FROM v_corr_view_remote));

DROP VIEW v_corr_view_nested;
DROP VIEW v_corr_view_remote;
DROP VIEW v_corr_view_local;
DROP TABLE t_corr_view_dist;
DROP TABLE t_corr_view_local;
