-- Tags: no-fasttest, no-parallel

DROP USER IF EXISTS test_user_03593;
DROP USER IF EXISTS test_user_03593_1;


CREATE USER test_user_03593;
CREATE USER test_user_03593_1;

GRANT READ,WRITE ON S3 TO test_user_03593;
GRANT READ,WRITE ON URL TO test_user_03593;

GRANT READ ON POSTGRES TO test_user_03593;
GRANT WRITE ON ODBC TO test_user_03593;

GRANT SET DEFINER ON test_user_03593_1 TO test_user_03593;

GRANT TABLE ENGINE ON TinyLog TO test_user_03593;

SELECT access_type, access_object FROM system.grants WHERE user_name='test_user_03593';

DROP USER test_user_03593;
DROP USER test_user_03593_1;
