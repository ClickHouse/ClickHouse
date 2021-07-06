# system.functions {#system-functions}

Contains information about normal and aggregate functions.

Columns:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

**Example**

```sql
 SELECT * FROM system.functions LIMIT 10;
```

```text
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

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/functions) <!--hide-->
