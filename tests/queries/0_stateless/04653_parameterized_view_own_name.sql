-- Tags: need-query-parameters

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

SELECT '-- the parameter type is irrelevant';
SELECT pv2.host_id FROM pv2(tenant = 't1');

SELECT '-- controls: an alias or no qualifier always worked';
SELECT p.host_id FROM pv(tenants = ['t1']) AS p;
SELECT host_id FROM pv(tenants = ['t1']);

SELECT '-- controls: a real table function must not gain a bindable name';
SELECT numbers.number FROM numbers(3); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM local_data JOIN numbers(3) ON 1 = 1; -- { serverError ALIAS_REQUIRED }

DROP VIEW pv2;
DROP VIEW pv;
DROP TABLE local_data;

-- A call qualified with an explicit database name binds by the view name too.
-- The database must be spelled literally: a query parameter is not accepted in the
-- database position of any table function (a pre-existing parser limitation).
DROP DATABASE IF EXISTS test_04653;
CREATE DATABASE test_04653;
CREATE TABLE test_04653.local_data (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO test_04653.local_data SELECT 't1', 1;
CREATE VIEW test_04653.pv AS SELECT tenant_id, host_id FROM test_04653.local_data WHERE tenant_id IN ({tenants:Array(String)});

SELECT '-- a database-qualified call binds by the view name too';
SELECT pv.host_id FROM test_04653.pv(tenants = ['t1']);
SELECT test_04653.pv.host_id FROM test_04653.pv(tenants = ['t1']);

DROP DATABASE test_04653;
