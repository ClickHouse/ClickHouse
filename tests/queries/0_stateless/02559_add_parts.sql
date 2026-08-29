-- Check MergeTree declaration in new format
CREATE TABLE check_system_tables
  (
    name1 UInt8,
    name2 UInt8,
    name3 UInt8
  ) ENGINE = MergeTree()
    ORDER BY name1
    PARTITION BY name2
    SAMPLE BY name1
    SETTINGS min_bytes_for_wide_part = 0, compress_marks=false, compress_primary_key=false;

SELECT parts, active_parts,total_marks FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables VALUES (1, 1, 1);
SELECT parts, active_parts,total_marks FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
INSERT INTO check_system_tables VALUES (1, 2, 1);
SELECT parts, active_parts,total_marks FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
ALTER TABLE check_system_tables DETACH PARTITION 1;
SELECT parts, active_parts,total_marks FROM system.tables WHERE name = 'check_system_tables' AND database = currentDatabase();
DROP TABLE IF EXISTS check_system_tables;
