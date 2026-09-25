-- Tags: use-rocksdb

DROP TABLE IF EXISTS kv_rls;
DROP TABLE IF EXISTS probe_rls;

CREATE TABLE kv_rls (key UInt64, tenant String, secret String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO kv_rls VALUES (1, 'public', 'public-1'), (2, 'hidden', 'hidden-2'), (3, 'public', 'public-3'), (4, 'hidden', 'hidden-4');

CREATE TABLE probe_rls (key UInt64) ENGINE = TinyLog;
INSERT INTO probe_rls VALUES (1), (2), (3), (5);

CREATE ROW POLICY kv_rls_public ON kv_rls FOR SELECT USING tenant = 'public' TO CURRENT_USER;

SET join_algorithm = 'direct,hash';
SET join_use_nulls = 0;

SELECT '-- plain select';
SELECT key, tenant, secret FROM kv_rls ORDER BY key;

SELECT '-- inner';
SELECT p.key, kv.tenant, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- inner, policy column not selected';
SELECT p.key, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left';
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left, join_use_nulls';
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key SETTINGS join_use_nulls = 1;

SELECT '-- left any';
SELECT p.key, kv.secret FROM probe_rls AS p LEFT ANY JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left semi';
SELECT p.key FROM probe_rls AS p LEFT SEMI JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- left anti';
SELECT p.key FROM probe_rls AS p LEFT ANTI JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;

SELECT '-- key space enumeration';
SELECT kv.key, kv.secret FROM numbers(10) AS n INNER JOIN kv_rls AS kv ON kv.key = n.number ORDER BY kv.key;

SELECT '-- using';
SELECT key, secret FROM probe_rls INNER JOIN kv_rls USING (key) ORDER BY key;

SELECT '-- no direct join under a row policy';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT p.key, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key
) WHERE explain LIKE '%DirectKeyValueJoin%';

SELECT '-- policy and additional_table_filters';
SELECT p.key, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key
    SETTINGS additional_table_filters = {'kv_rls': 'key != 1'};
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key
    SETTINGS additional_table_filters = {'kv_rls': 'key != 1'};

DROP ROW POLICY kv_rls_public ON kv_rls;

SELECT '-- direct join without filters';
SELECT p.key, kv.tenant, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key;
SELECT extract(explain, 'Algorithm: \\w+') FROM (
    EXPLAIN actions = 1
    SELECT p.key, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key
) WHERE explain LIKE '%Algorithm:%';

SELECT '-- additional_table_filters without a policy';
SELECT p.key, kv.tenant, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key
    SETTINGS additional_table_filters = {'kv_rls': 'tenant = \'hidden\''};
SELECT p.key, kv.key, kv.secret FROM probe_rls AS p LEFT JOIN kv_rls AS kv ON kv.key = p.key ORDER BY p.key
    SETTINGS additional_table_filters = {'kv_rls': 'tenant = \'hidden\''};
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT p.key, kv.secret FROM probe_rls AS p INNER JOIN kv_rls AS kv ON kv.key = p.key
    SETTINGS additional_table_filters = {'kv_rls': 'tenant = \'hidden\''}
) WHERE explain LIKE '%DirectKeyValueJoin%';

DROP TABLE kv_rls;
DROP TABLE probe_rls;
