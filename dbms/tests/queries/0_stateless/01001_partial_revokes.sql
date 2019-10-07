DROP ROLE IF EXISTS r1, r2, r3;
CREATE ROLE r1, r2, r3;

GRANT SELECT ON *.* TO r1;
SELECT '1)';
SHOW GRANTS FOR r1;
SET partial_revokes = 0;
REVOKE SELECT ON testdb.* FROM r1;
SELECT '2)';
SHOW GRANTS FOR r1;
SET partial_revokes = 1;
REVOKE SELECT ON testdb.* FROM r1;
SELECT '3)';
SHOW GRANTS FOR r1;

GRANT SELECT ON testdb.* TO r2;
SELECT '4)';
SHOW GRANTS FOR r2;
SET partial_revokes = 0;
REVOKE SELECT ON testdb.testtable FROM r2;
SELECT '5)';
SHOW GRANTS FOR r2;
SET partial_revokes = 1;
REVOKE SELECT ON testdb.testtable FROM r2;
SELECT '6)';
SHOW GRANTS FOR r2;

GRANT SELECT ON testdb.testtable TO r3;
SELECT '7)';
SHOW GRANTS FOR r3;
SET partial_revokes = 0;
REVOKE SELECT(x) ON testdb.testtable FROM r3;
SELECT '8)';
SHOW GRANTS FOR r3;
SET partial_revokes = 1;
REVOKE SELECT(x) ON testdb.testtable FROM r3;
SELECT '9)';
SHOW GRANTS FOR r3;

DROP ROLE r1, r2, r3;
