DROP USER IF EXISTS test_user_01300_create, test_user_01300_alter;

SELECT 'CREATE';
CREATE USER test_user_01300_create ALLOW PROXYING VIA foobar;
SHOW CREATE USER test_user_01300_create;

SELECT 'ALTER';
CREATE USER test_user_01300_alter;
SHOW CREATE USER test_user_01300_alter;
ALTER USER test_user_01300_alter ALLOW PROXYING VIA foobar;
SHOW CREATE USER test_user_01300_alter;
ALTER USER test_user_01300_alter DENY PROXYING VIA foobar;
SHOW CREATE USER test_user_01300_alter;
