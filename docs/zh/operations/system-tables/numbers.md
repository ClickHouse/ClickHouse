# system.numbers {#system-numbers}

这个表有一个名为 `number` 的 UInt64 列，包含了几乎所有从 0 开始的自然数。

你可以用这个表进行测试，或者如果你需要进行暴力搜索。

从该表的读取是不并行的。

**示例**

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

[原文](https://clickhouse.com/docs/zh/operations/system-tables/numbers) <!--hide-->
