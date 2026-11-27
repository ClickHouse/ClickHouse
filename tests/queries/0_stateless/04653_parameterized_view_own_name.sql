-- Tags: need-query-parameters

-- The fix is in the query-tree analyzer, so pin it: the `old analyzer` CI jobs link
-- `users.d/analyzer.xml` and the randomized `compatibility='<24.3'` setting also reverts
-- `allow_experimental_analyzer`, and on the legacy path a JOINed parameterized view is not
-- expanded at all (only the left table storage is), so these rows would assert a different
-- code path's behaviour.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS local_data;
DROP VIEW IF EXISTS pv;
DROP VIEW IF EXISTS pv2;

CREATE TABLE local_data (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO local_data SELECT 't1', 1;

CREATE VIEW pv AS SELECT tenant_id, host_id FROM local_data WHERE tenant_id IN ({tenants:Array(String)});
CREATE VIEW pv2 AS SELECT tenant_id, host_id FROM local_data WHERE tenant_id = {tenant:String};

SELECT '-- a parameterized view may be joined without an alias';
SELECT count() FROM local_data JOIN pv(tenants = ['t1']) USING (tenant_id);
SELECT count() FROM local_data AS t JOIN pv(tenants = ['t1']) ON t.tenant_id = pv.tenant_id;
SELECT count() FROM local_data, pv(tenants = ['t1']);
SELECT count() FROM pv(tenants = ['t1']) JOIN local_data AS t USING (tenant_id);

SELECT '-- its columns may be qualified with the view name';
SELECT pv.host_id FROM pv(tenants = ['t1']);
SELECT host_id FROM pv(tenants = ['t1']) WHERE pv.host_id = 1;
SELECT pv.tenant_id FROM pv(tenants = ['t1']) GROUP BY pv.tenant_id;
SELECT pv.host_id FROM pv(tenants = ['t1']) ORDER BY pv.host_id;
SELECT {CLICKHOUSE_DATABASE:Identifier}.pv.host_id FROM pv(tenants = ['t1']);
SELECT count() FROM local_data AS t JOIN pv(tenants = ['t1']) ON t.tenant_id = pv.tenant_id SETTINGS joined_subquery_requires_alias = 0;

SELECT '-- matcher-expanded columns are qualified with the view name';
DESCRIBE (SELECT * FROM local_data, pv(tenants = ['t1']));
DESCRIBE (SELECT * FROM local_data JOIN pv(tenants = ['t1']) USING (tenant_id));

SELECT '-- control: a real table function contributes no qualifier';
DESCRIBE (SELECT * FROM local_data AS t JOIN numbers(3) AS n ON 1 = 1);

SELECT '-- the parameter type is irrelevant';
SELECT pv2.host_id FROM pv2(tenant = 't1');

SELECT '-- controls: an alias or no qualifier always worked';
SELECT p.host_id FROM pv(tenants = ['t1']) AS p;
SELECT host_id FROM pv(tenants = ['t1']);

SELECT '-- controls: a regular table function and an ordinary view must not gain a bindable name';
SELECT numbers.number FROM numbers(3); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM local_data JOIN numbers(3) ON 1 = 1; -- { serverError ALIAS_REQUIRED }
SELECT view.dummy FROM view(SELECT 1 AS dummy); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM local_data JOIN view(SELECT 1 AS dummy) ON 1 = 1; -- { serverError ALIAS_REQUIRED }
-- `tenant_id` collides with `local_data`'s, so the matcher must decide whether to qualify it;
-- an ordinary view contributes no qualification parts, so the second one stays bare.
DESCRIBE (SELECT * FROM local_data, view(SELECT 't1' AS tenant_id)) SETTINGS joined_subquery_requires_alias = 0;

DROP VIEW pv2;
DROP VIEW pv;
DROP TABLE local_data;

-- The view also binds by its own name when it lives outside the session's current database.
-- The call itself stays unqualified: a query parameter is not accepted in the database
-- position of a table function, and a literal database name would not be parallel-safe.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE local_data (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO local_data SELECT 't1', 1;
CREATE VIEW pv AS SELECT tenant_id, host_id FROM local_data WHERE tenant_id IN ({tenants:Array(String)});

SELECT '-- a view in another database binds by the view name too';
SELECT pv.host_id FROM pv(tenants = ['t1']);
SELECT {CLICKHOUSE_DATABASE_1:Identifier}.pv.host_id FROM pv(tenants = ['t1']);

SELECT '-- control: qualifying with a database that does not hold the view does not bind';
SELECT {CLICKHOUSE_DATABASE:Identifier}.pv.host_id FROM pv(tenants = ['t1']); -- { serverError UNKNOWN_IDENTIFIER }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
