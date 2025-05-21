-- Tags: no-parallel
-- Do not run this test in parallel because creating the same resource twice will fail
CREATE OR REPLACE RESOURCE 03232_resource_1 (WRITE DISK 03232_disk_1, READ DISK 03232_disk_1);
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '03232_%' ORDER BY name;
CREATE RESOURCE IF NOT EXISTS 03232_resource_2 (READ DISK 03232_disk_2);
CREATE RESOURCE 03232_resource_3 (WRITE DISK 03232_disk_2);
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '03232_%' ORDER BY name;
DROP RESOURCE IF EXISTS 03232_resource_2;
DROP RESOURCE 03232_resource_3;
SELECT name, read_disks, write_disks, create_query FROM system.resources WHERE name ILIKE '03232_%' ORDER BY name;
DROP RESOURCE 03232_resource_1;
