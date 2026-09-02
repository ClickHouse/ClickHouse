-- Tags: no-parallel

DROP USER IF EXISTS test_user_01073;
DROP ROLE IF EXISTS test_role_01073;

SELECT 'A';
CREATE USER test_user_01073;
SHOW CREATE USER test_user_01073;

SELECT 'B';
SHOW GRANTS FOR test_user_01073;

SELECT 'C';
GRANT SELECT ON db1.* TO test_user_01073;
GRANT SELECT ON db2.table TO test_user_01073;
GRANT SELECT(col1) ON db3.table TO test_user_01073;
GRANT SELECT(col1, col2) ON db4.table TO test_user_01073;
GRANT INSERT ON *.* TO test_user_01073;
GRANT DELETE ON *.* TO test_user_01073;
GRANT SELECT(col1) ON *.* TO test_user_01073; -- { clientError SYNTAX_ERROR }
GRANT SELECT(col1) ON db1.* TO test_user_01073; -- { clientError SYNTAX_ERROR }
GRANT INSERT(col1, col2) ON db1.* TO test_user_01073; -- { clientError SYNTAX_ERROR }
SHOW GRANTS FOR test_user_01073;

SELECT 'D';
REVOKE SELECT ON db1.* FROM test_user_01073;
REVOKE SELECT ON db2.table FROM test_user_01073;
REVOKE SELECT ON db3.table FROM test_user_01073;
REVOKE SELECT(col2) ON db4.table FROM test_user_01073;
REVOKE INSERT ON *.* FROM test_user_01073;
SHOW GRANTS FOR test_user_01073;

SELECT 'E';
CREATE ROLE test_role_01073;
GRANT SELECT ON db1.* TO test_role_01073;
REVOKE SELECT(c1, c2, c3, c4, c5) ON db1.table1 FROM test_role_01073;
REVOKE SELECT(c1) ON db1.table2 FROM test_role_01073;
SHOW GRANTS FOR test_role_01073;

DROP USER test_user_01073;
DROP ROLE test_role_01073;
