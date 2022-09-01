# system.functions {#system-functions}

包含有关常规函数和聚合函数的信息。

列:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

**举例**
```
 SELECT * FROM system.functions LIMIT 10;
```

```
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
