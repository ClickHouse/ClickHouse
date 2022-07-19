# system.numbers {#system-numbers}

This table contains a single UInt64 column named `number` that contains almost all the natural numbers starting from zero.

You can use this table for tests, or if you need to do a brute force search.

Reads from this table are not parallelized.

**Example**

```sql
:) SELECT * FROM system.numbers LIMIT 10;
```

```text
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘

10 rows in set. Elapsed: 0.001 sec.
```

[Original article](https://clickhouse.com/docs/en/operations/system-tables/numbers) <!--hide-->
