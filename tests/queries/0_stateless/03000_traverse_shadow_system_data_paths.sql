-- Tags: no-replicated-database, no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS 03000_traverse_shadow_system_data_path_table;

CREATE TABLE 03000_traverse_shadow_system_data_path_table (
    id Int64,
    data String
) ENGINE=MergeTree()
ORDER BY id
SETTINGS storage_policy='s3_cache';

INSERT INTO 03000_traverse_shadow_system_data_path_table VALUES (0, 'data');
ALTER TABLE 03000_traverse_shadow_system_data_path_table FREEZE WITH NAME '03000_traverse_shadow_system_data_path_table_backup';

SELECT count() > 0
FROM system.remote_data_paths 
WHERE disk_name = 's3_cache' AND local_path LIKE '%shadow/03000_traverse_shadow_system_data_path_table_backup%'
SETTINGS traverse_shadow_remote_data_paths=1;

DROP TABLE IF EXISTS 03000_traverse_shadow_system_data_path_table;
