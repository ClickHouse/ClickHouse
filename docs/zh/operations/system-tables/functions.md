# system.functions {#system-functions}

包含有关常规函数和聚合函数的信息。

列:

-   `name`(`String`) – 函数的名称。
-   `is_aggregate`(`UInt8`) — 该函数是否聚合。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/functions) <!--hide-->

**示例**

```sql
 SELECT * FROM system.functions LIMIT 10;
```

```text
>>>>>>> update zh translations for system tables and some pages related to access rights
┌─name─────────────────────┬─is_aggregate─┬─case_insensitive─┬─alias_to─┐
│ sumburConsistentHash     │            0 │                0 │          │
│ yandexConsistentHash     │            0 │                0 │          │
│ demangle                 │            0 │                0 │          │
│ addressToLine            │            0 │                0 │          │
│ JSONExtractRaw           │            0 │                0 │          │
│ JSONExtractKeysAndValues │            0 │                0 │          │
│ JSONExtract              │            0 │                0 │          │
│ JSONExtractString        │            0 │                0 │          │
│ JSONExtractFloat         │            0 │                0 │          │
│ JSONExtractInt           │            0 │                0 │          │
└──────────────────────────┴──────────────┴──────────────────┴──────────┘

10 rows in set. Elapsed: 0.002 sec. 
```
