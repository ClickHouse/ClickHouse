-- Test: exercises `StorageSystemDNSCache::fillData` row insertion.
-- The existing 02998_system_dns_cache_table.sql only checks the schema with LIMIT 0.
-- Integration tests (test_dns_cache) read `system.dns_cache` but only check `hostname` and `ip_address`,
-- never `cached_at` (DateTime population) or `ip_family` (Enum8 mapping).
-- This test deterministically populates the cache via a `remote(localhost,...)` call
-- and then verifies a row exists with a populated `cached_at` and a valid `ip_family` enum value.
-- Covers:
--   src/Storages/System/StorageSystemDNSCache.cpp:55  res_columns->insert(address.family())
--   src/Storages/System/StorageSystemDNSCache.cpp:56  insert(static_cast<UInt32>(to_time_t(entry.cached_at)))

-- Force a DNS lookup of localhost so the host cache is populated.
SELECT count() FROM remote('localhost', system, one) FORMAT Null;

-- A row for localhost must exist with a non-epoch `cached_at` and a valid `ip_family` enum value.
-- Tolerant to parallel `SYSTEM CLEAR DNS CACHE` from other tests: just checks at least one matching row exists.
SELECT count() > 0 FROM system.dns_cache
WHERE hostname = 'localhost'
  AND ip_family IN ('IPv4', 'IPv6')
  AND cached_at > '2020-01-01 00:00:00';
