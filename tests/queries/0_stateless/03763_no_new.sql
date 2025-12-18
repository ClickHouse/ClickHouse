SELECT throwIf(name != '', 'No settings can be named "new"') FROM system.settings WHERE name LIKE '%new\_%' AND name NOT LIKE '%new\_file%';
SELECT throwIf(name != '', 'No settings can be named "new"') FROM system.merge_tree_settings WHERE name LIKE '%new\_%' AND name NOT LIKE '%new\_file%';
