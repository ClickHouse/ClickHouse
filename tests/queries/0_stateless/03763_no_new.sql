-- Checks that setting names don't contain "new"

SELECT throwIf(name != '', 'Settings must not contain "new"')
FROM system.settings
WHERE
    name LIKE '%new\_%'
    AND name NOT LIKE '%_create_new\_file%'
    AND alias_for == '';
-- Intentionally ignore setting aliases (column "alias_for").
-- We need aliases as a backdoor to fix setting names with "new":
-- - 1) Introduce a new setting without "new".
-- - 2) Make the existing setting with "new" an alias of the new setting.

SELECT throwIf(name != '', 'Settings must not contain "new"')
FROM system.merge_tree_settings
WHERE
    name LIKE '%new\_%'
    AND name NOT LIKE '%new\_file%';

SELECT throwIf(name != '', 'Settings must not contain "new"')
FROM system.server_settings
WHERE
    name LIKE '%new\_%'
    AND name NOT LIKE '%new\_file%';
