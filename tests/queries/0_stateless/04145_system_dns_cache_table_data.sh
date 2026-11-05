#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Exercises StorageSystemDNSCache::fillData row population (not just the schema):
#   src/Storages/System/StorageSystemDNSCache.cpp:55  res_columns->insert(address.family())                 -> ip_family Enum8
#   src/Storages/System/StorageSystemDNSCache.cpp:56  insert(static_cast<UInt32>(to_time_t(entry.cached_at))) -> cached_at DateTime
# The existing 02998_system_dns_cache_table.sql only checks the schema (LIMIT 0); the
# test_dns_cache integration test reads the table but never asserts cached_at / ip_family.
#
# Repopulate-and-check in a loop so a concurrent `SYSTEM CLEAR DNS CACHE` from another
# parallel stateless test cannot wipe the localhost row in the window between the
# population query and the assertion. Each iteration re-primes the cache, then checks.

result=0
for _ in {1..30}; do
    # Force a DNS lookup of localhost so the host cache is (re)populated.
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM remote('localhost', system, one) FORMAT Null"
    result=$($CLICKHOUSE_CLIENT -q "
        SELECT count() > 0 FROM system.dns_cache
        WHERE hostname = 'localhost'
          AND ip_family IN ('IPv4', 'IPv6')
          AND cached_at > '2020-01-01 00:00:00'")
    [ "$result" = "1" ] && break
    sleep 0.3
done

echo "$result"
