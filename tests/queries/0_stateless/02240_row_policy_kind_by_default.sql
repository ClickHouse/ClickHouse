DROP ROW POLICY IF EXISTS 02240_r1, 02240_r2, 02240_r3, 02240_r4 ON db1.table1;

SELECT concat('rbac_version=', toString(getSetting('rbac_version')));
CREATE ROW POLICY 02240_r1 ON db1.table1;
CREATE ROW POLICY 02240_r2 ON db1.table1 AS permissive;
CREATE ROW POLICY 02240_r3 ON db1.table1 AS restrictive;
CREATE ROW POLICY 02240_r4 ON db1.table1 AS simple;
SHOW CREATE ROW POLICY 02240_r1, 02240_r2, 02240_r3, 02240_r4 ON db1.table1;
SELECT short_name, kind FROM system.row_policies WHERE short_name LIKE '02240%' ORDER BY short_name;
DROP ROW POLICY 02240_r1, 02240_r2, 02240_r3, 02240_r4 ON db1.table1;

SET rbac_version=2;
SELECT concat('rbac_version=', toString(getSetting('rbac_version')));
CREATE ROW POLICY 02240_r1 ON db1.table1;
CREATE ROW POLICY 02240_r2 ON db1.table1 AS permissive;
CREATE ROW POLICY 02240_r3 ON db1.table1 AS restrictive;
CREATE ROW POLICY 02240_r4 ON db1.table1 AS simple;
SHOW CREATE ROW POLICY 02240_r1, 02240_r2, 02240_r3, 02240_r4 ON db1.table1;
SELECT short_name, kind FROM system.row_policies WHERE short_name LIKE '02240%' ORDER BY short_name;
DROP ROW POLICY 02240_r1, 02240_r2, 02240_r3, 02240_r4 ON db1.table1;

SET rbac_version=1;
