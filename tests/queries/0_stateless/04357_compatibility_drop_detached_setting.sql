SET compatibility = '26.5';
SELECT name, value, changed FROM system.settings WHERE name = 'allow_experimental_drop_detached_table';
