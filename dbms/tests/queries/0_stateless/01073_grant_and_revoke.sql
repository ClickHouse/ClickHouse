DROP USER IF EXISTS test_user_01073;

CREATE USER test_user_01073;
SHOW CREATE USER test_user_01073;

SELECT 'A';
SHOW GRANTS FOR test_user_01073;

GRANT SELECT ON db1.* TO test_user_01073;
GRANT SELECT ON db2.table TO test_user_01073;
GRANT SELECT(col1) ON db3.table TO test_user_01073;
GRANT SELECT(col1, col2) ON db4.table TO test_user_01073;
GRANT INSERT ON *.* TO test_user_01073;
GRANT DELETE ON *.* TO test_user_01073;

SELECT 'B';
SHOW GRANTS FOR test_user_01073;

REVOKE SELECT ON db1.* FROM test_user_01073;
REVOKE SELECT ON db2.table FROM test_user_01073;
REVOKE SELECT ON db3.table FROM test_user_01073;
REVOKE SELECT(col2) ON db4.table FROM test_user_01073;
REVOKE INSERT ON *.* FROM test_user_01073;

SELECT 'C';
SHOW GRANTS FOR test_user_01073;

DROP USER test_user_01073;
