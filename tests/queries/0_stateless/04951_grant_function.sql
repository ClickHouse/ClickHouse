-- Tags: no-parallel, no-replicated-database

DROP USER IF EXISTS test_user_04951;

CREATE USER test_user_04951;

GRANT FUNCTION ON hex TO test_user_04951;
GRANT FUNCTION ON decrypt TO test_user_04951;

SHOW GRANTS FOR test_user_04951;
SELECT access_type, access_object FROM system.grants WHERE user_name = 'test_user_04951' ORDER BY access_type, access_object;

GRANT FUNCTION ON * TO test_user_04951;
SHOW GRANTS FOR test_user_04951;

REVOKE FUNCTION ON decrypt FROM test_user_04951;
SHOW GRANTS FOR test_user_04951;

DROP USER test_user_04951;
