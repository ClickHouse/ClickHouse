# functions {#system-functions}

Contains information about normal and aggregate functions.

Columns:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

**Example**

```sql
 SELECT * FROM system.functions LIMIT 10;
```

```text
┌─name──────────────────┬─is_aggregate─┬─case_insensitive─┬─alias_to─┬─create_query─┬─origin─┐
│ logTrace              │            0 │                0 │          │              │ System │
│ aes_decrypt_mysql     │            0 │                0 │          │              │ System │
│ aes_encrypt_mysql     │            0 │                0 │          │              │ System │
│ decrypt               │            0 │                0 │          │              │ System │
│ encrypt               │            0 │                0 │          │              │ System │
│ toBool                │            0 │                0 │          │              │ System │
│ windowID              │            0 │                0 │          │              │ System │
│ hopStart              │            0 │                0 │          │              │ System │
│ hop                   │            0 │                0 │          │              │ System │
│ snowflakeToDateTime64 │            0 │                0 │          │              │ System │
└───────────────────────┴──────────────┴──────────────────┴──────────┴──────────────┴────────┘

10 rows in set. Elapsed: 0.002 sec.
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/functions) <!--hide-->
