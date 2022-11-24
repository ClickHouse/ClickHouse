---
slug: /en/operations/system-tables/dns_cache
---
# dns_cache

This table contains one entry per (hostname, ip address) pair in the internal dns cache.

Columns:

-    `hostname` ([String](../../sql-reference/data-types/string.md)) — cached hostname
-    `ip_address` ([String](../../sql-reference/data-types/string.md)) — cached ip address for the hostname
-    `family` ([String](../../sql-reference/data-types/string.md)) — family of ip address: `IPv4`, `IPv6`, or `UNIX_LOCAL`.

**Example**

```sql
:) SELECT * FROM system.dns_cache;
```

```text
┌─hostname───────────────┬─ip_address───────────────┬─family─┐
│ localhost              │ 127.0.0.1                │ IPv4   │
│ storage.googleapis.com │ 142.250.217.112          │ IPv4   │
│ storage.googleapis.com │ 142.251.215.240          │ IPv4   │
│ storage.googleapis.com │ 172.217.14.208           │ IPv4   │
│ storage.googleapis.com │ 172.217.14.240           │ IPv4   │
│ storage.googleapis.com │ 142.250.69.208           │ IPv4   │
│ storage.googleapis.com │ 142.251.33.112           │ IPv4   │
│ storage.googleapis.com │ 142.251.211.240          │ IPv4   │
│ storage.googleapis.com │ 142.251.33.80            │ IPv4   │
│ storage.googleapis.com │ 142.250.217.80           │ IPv4   │
│ storage.googleapis.com │ 2607:f8b0:400a:80b::2010 │ IPv6   │
│ storage.googleapis.com │ 2607:f8b0:400a:800::2010 │ IPv6   │
│ storage.googleapis.com │ 2607:f8b0:400a:801::2010 │ IPv6   │
│ storage.googleapis.com │ 2607:f8b0:400a:803::2010 │ IPv6   │
└────────────────────────┴──────────────────────────┴────────┘

14 rows in set. Elapsed: 0.001 sec.
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/dns_cache) <!--hide-->
