DROP USER IF EXISTS test_user_01074;
CREATE USER test_user_01074;

SELECT 'A';
SET partial_revokes=0;
GRANT SELECT ON *.* TO test_user_01074;
REVOKE SELECT ON db.* FROM test_user_01074;
SHOW GRANTS FOR test_user_01074;

SELECT 'B';
SET partial_revokes=1;
REVOKE SELECT ON db.* FROM test_user_01074;
SHOW GRANTS FOR test_user_01074;

DROP USER test_user_01074;
