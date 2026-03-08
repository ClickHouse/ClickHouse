DROP USER IF EXISTS test_elan;
CREATE USER test_elan;

GRANT SELECT ON foo.bar TO test_elan;
GRANT SELECT ON foo.bar* TO test_elan;
GRANT SELECT ON db*.* TO test_elan;
REVOKE SELECT ON foo.bar FROM test_elan;

SELECT user_name, access_type, database, table, grant_scope FROM system.grants WHERE user_name = 'test_elan' ORDER BY database, table, grant_scope;

DROP USER test_elan;
