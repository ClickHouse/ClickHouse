# system.numbers {#system-numbers}

此表包含一个名为 `number` 的UInt64的列，它包含几乎所有从零开始的自然数。

您可以使用此表进行测试，或者如果您需要进行蛮力搜索。

从此表中读取是不并行化的。

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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/numbers) <!--hide-->
