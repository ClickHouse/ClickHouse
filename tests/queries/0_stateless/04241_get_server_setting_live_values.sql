-- Verify that getServerSetting returns the live value for settings that can change at runtime,
-- matching what system.server_settings reports for the same setting.

-- max_server_memory_usage defaults to 0 in the config but is auto-computed at startup from
-- max_server_memory_usage_to_ram_ratio * total RAM. system.server_settings reports the
-- computed limit; getServerSetting should report the same number.
SELECT toUInt64(getServerSetting('max_server_memory_usage'))
     = toUInt64((SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'))
     AS max_server_memory_usage_matches;

SELECT toUInt64(getServerSetting('max_concurrent_queries'))
     = toUInt64((SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'))
     AS max_concurrent_queries_matches;

SELECT toUInt64(getServerSetting('mark_cache_size'))
     = toUInt64((SELECT value FROM system.server_settings WHERE name = 'mark_cache_size'))
     AS mark_cache_size_matches;

-- Settings without a live override should still work and agree with system.server_settings.
SELECT toString(getServerSetting('mark_cache_policy'))
     = (SELECT value FROM system.server_settings WHERE name = 'mark_cache_policy')
     AS mark_cache_policy_matches;

-- keeper_hosts is not stored in the config; it is derived from the <zookeeper> config at query time.
-- system.server_settings reports the live endpoints; getServerSetting should report the same value.
SELECT toString(getServerSetting('keeper_hosts'))
     = (SELECT value FROM system.server_settings WHERE name = 'keeper_hosts')
     AS keeper_hosts_matches;
