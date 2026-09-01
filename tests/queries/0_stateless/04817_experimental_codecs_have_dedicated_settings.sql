-- Every experimental codec must have a dedicated `enable_<codec>_codec` setting.

SELECT name FROM system.codecs
WHERE is_experimental AND concat('enable_', lower(name), '_codec') NOT IN (SELECT name FROM system.settings);
