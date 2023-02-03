# one

This table contains a single row with a single `dummy` UInt8 column containing the value 0.

This table is used if a `SELECT` query does not specify the `FROM` clause.

This is similar to the `DUAL` table found in other DBMSs.

**Example**

```sql
:) SELECT * FROM system.one LIMIT 10;
```

```text
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/one) <!--hide-->
