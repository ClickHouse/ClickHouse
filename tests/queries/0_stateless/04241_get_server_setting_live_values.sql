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

-- Path-style aliases for runtime-changeable settings should also resolve to the live value.
-- `query_cache.max_size_in_bytes` is an alias for `query_cache_max_size_in_bytes`.
SELECT toUInt64(getServerSetting('query_cache.max_size_in_bytes'))
     = toUInt64((SELECT value FROM system.server_settings WHERE name = 'query_cache_max_size_in_bytes'))
     AS query_cache_max_size_in_bytes_alias_matches;
