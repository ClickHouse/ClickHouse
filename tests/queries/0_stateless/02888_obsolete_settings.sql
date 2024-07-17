SELECT '-- Obsolete server settings';
SELECT name FROM system.server_settings WHERE is_obsolete = 1 ORDER BY name;

SELECT '-- Obsolete general settings';
SELECT count() >= 10 FROM system.settings WHERE is_obsolete = 1;

SELECT '-- Obsolete merge tree settings';
SELECT count() >= 10 FROM system.merge_tree_settings WHERE is_obsolete = 1;
