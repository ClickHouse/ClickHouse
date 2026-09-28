SELECT throwIf(name != '', 'No settings can be named "new"') FROM system.settings WHERE name LIKE '%new\_%' AND name NOT LIKE '%new\_file%' AND name NOT IN (select alias_for from system.settings where name not like '%new\_%');
SELECT throwIf(name != '', 'No settings can be named "new"') FROM system.merge_tree_settings WHERE name LIKE '%new\_%' AND name NOT LIKE '%new\_file%';
SELECT throwIf(name != '', 'No settings can be named "new"') FROM system.server_settings WHERE name LIKE '%new\_%' AND name NOT LIKE '%new\_file%';
