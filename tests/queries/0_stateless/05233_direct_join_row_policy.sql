-- Tags: use-rocksdb

SET enable_analyzer = 0;
SET join_algorithm = 'direct,hash';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS kv_rls;
DROP TABLE IF EXISTS join_rls;
DROP TABLE IF EXISTS probe_rls;

CREATE TABLE kv_rls (key UInt64, tenant String, secret String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO kv_rls VALUES (1, 'public', 'public-1'), (2, 'hidden', 'hidden-2'), (3, 'public', 'public-3'), (4, 'hidden', 'hidden-4');

CREATE TABLE join_rls (key UInt64, tenant String, secret String) ENGINE = Join(ANY, LEFT, key);
INSERT INTO join_rls VALUES (1, 'public', 'public-1'), (2, 'hidden', 'hidden-2'), (3, 'public', 'public-3'), (4, 'hidden', 'hidden-4');

CREATE TABLE probe_rls (key UInt64) ENGINE = TinyLog;
INSERT INTO probe_rls VALUES (1), (2), (3), (5);

CREATE ROW POLICY kv_rls_public ON kv_rls FOR SELECT USING tenant = 'public' TO CURRENT_USER;
CREATE ROW POLICY join_rls_public ON join_rls FOR SELECT USING tenant = 'public' TO CURRENT_USER;

SELECT '-- plain select';
SELECT key, tenant, secret FROM kv_rls ORDER BY key;

SELECT '-- inner';
SELECT p.key, kv.tenant, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left';
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left, join_use_nulls';
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key SETTINGS join_use_nulls = 1;

SELECT '-- left semi';
SELECT p.key FROM probe_rls AS p LEFT SEMI JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left anti';
SELECT p.key FROM probe_rls AS p LEFT ANTI JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- join table';
SELECT p.key, j.secret FROM probe_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key;

DROP ROW POLICY kv_rls_public ON kv_rls;
DROP ROW POLICY join_rls_public ON join_rls;

SELECT '-- without a policy';
SELECT p.key, kv.tenant, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;
SELECT p.key, j.secret FROM probe_rls AS p LEFT ANY JOIN join_rls AS j ON j.key = p.key ORDER BY p.key;

DROP TABLE kv_rls;
DROP TABLE join_rls;
DROP TABLE probe_rls;
