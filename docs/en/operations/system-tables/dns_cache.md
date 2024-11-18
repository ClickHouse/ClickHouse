---
slug: /en/operations/system-tables/dns_cache
---
# dns_cache

Contains information about cached DNS records.

Columns:

- `hostname` ([String](../../sql-reference/data-types/string.md)) — cached hostname
- `ip_address` ([String](../../sql-reference/data-types/string.md)) — ip address for the hostname
- `ip_family` ([Enum](../../sql-reference/data-types/enum.md)) — family of the ip address, possible values: 
   - 'IPv4' 
   - 'IPv6'
   - 'UNIX_LOCAL'
- `cached_at` ([DateTime](../../sql-reference/data-types/datetime.md)) - when the record was cached

**Example**

Query:

```sql
SELECT * FROM system.dns_cache;
```

Result:

| hostname | ip\_address | ip\_family | cached\_at |
| :--- | :--- | :--- | :--- |
| localhost | ::1 | IPv6 | 2024-02-11 17:04:40 |
| localhost | 127.0.0.1 | IPv4 | 2024-02-11 17:04:40 |

**See also**

- [disable_internal_dns_cache setting](../../operations/server-configuration-parameters/settings.md#disable_internal_dns_cache)
- [dns_cache_max_entries setting](../../operations/server-configuration-parameters/settings.md#dns_cache_max_entries)
- [dns_cache_update_period setting](../../operations/server-configuration-parameters/settings.md#dns_cache_update_period)
- [dns_max_consecutive_failures setting](../../operations/server-configuration-parameters/settings.md#dns_max_consecutive_failures)
