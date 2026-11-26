-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates and drops a global named collection
-- no-replicated-database: named collections are server-global and their DDL is not replicated

-- The `url = (...)` key-value argument fails identifier resolution and is left unresolved,
-- so set collection must not traverse into it; the scalar subquery over a parameterized view
-- is then evaluated as a constant expression, which runs `collectSets` on a tree containing
-- a parameterized view `TableFunctionNode`. The query fails on the invalid URL '0'.
DROP NAMED COLLECTION IF EXISTS nc_collect_sets_url_pv;
CREATE NAMED COLLECTION nc_collect_sets_url_pv AS format = 'CSV';
CREATE VIEW pv AS SELECT number FROM numbers({n:UInt64});

SELECT * FROM url(nc_collect_sets_url_pv, url = (SELECT toString(number) FROM pv(n=1))) FORMAT Null; -- { serverError BAD_ARGUMENTS }

DROP VIEW pv;
DROP NAMED COLLECTION nc_collect_sets_url_pv;
