-- Tags: no-random-settings
-- The columns cache is enabled by default in 26.11. The settings-history entry uses
-- `previous_value = false`, so `compatibility` with 26.10 or earlier turns it back off and
-- `compatibility` with 26.11 keeps it on. Check the default and the two neighbouring versions.

SELECT getSetting('use_columns_cache');
SELECT getSetting('use_columns_cache') SETTINGS compatibility = '26.10';
SELECT getSetting('use_columns_cache') SETTINGS compatibility = '26.11';
