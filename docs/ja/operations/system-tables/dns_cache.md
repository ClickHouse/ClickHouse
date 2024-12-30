---
slug: /ja/operations/system-tables/dns_cache
---
# dns_cache

キャッシュされたDNSレコードに関する情報を含みます。

カラム:

- `hostname` ([String](../../sql-reference/data-types/string.md)) — キャッシュされたホスト名
- `ip_address` ([String](../../sql-reference/data-types/string.md)) — ホスト名に対応するIPアドレス
- `ip_family` ([Enum](../../sql-reference/data-types/enum.md)) — IPアドレスのファミリー、可能な値:
   - 'IPv4' 
   - 'IPv6'
   - 'UNIX_LOCAL'
- `cached_at` ([DateTime](../../sql-reference/data-types/datetime.md)) - レコードがキャッシュされた日時

**例**

クエリ:

```sql
SELECT * FROM system.dns_cache;
```

結果:

| hostname | ip\_address | ip\_family | cached\_at |
| :--- | :--- | :--- | :--- |
| localhost | ::1 | IPv6 | 2024-02-11 17:04:40 |
| localhost | 127.0.0.1 | IPv4 | 2024-02-11 17:04:40 |

**関連項目**

- [disable_internal_dns_cache 設定](../../operations/server-configuration-parameters/settings.md#disable_internal_dns_cache)
- [dns_cache_max_entries 設定](../../operations/server-configuration-parameters/settings.md#dns_cache_max_entries)
- [dns_cache_update_period 設定](../../operations/server-configuration-parameters/settings.md#dns_cache_update_period)
- [dns_max_consecutive_failures 設定](../../operations/server-configuration-parameters/settings.md#dns_max_consecutive_failures)
