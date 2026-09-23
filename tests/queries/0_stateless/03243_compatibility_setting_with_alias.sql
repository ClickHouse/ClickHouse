-- `compatibility` reverts the default of a setting that has an alias, and the value it reverts to is
-- reported the same way under both names. The default of `enable_lightweight_update` (alias
-- `allow_experimental_lightweight_update`) was flipped to `1` in 25.8.
SET enable_lightweight_update = DEFAULT;
SELECT name, value, changed from system.settings where name IN ('enable_lightweight_update', 'allow_experimental_lightweight_update') ORDER BY name;
SET compatibility = '25.8';
SELECT name, value, changed from system.settings where name IN ('enable_lightweight_update', 'allow_experimental_lightweight_update') ORDER BY name;
SET compatibility = '25.7';
SELECT name, value, changed from system.settings where name IN ('enable_lightweight_update', 'allow_experimental_lightweight_update') ORDER BY name;
