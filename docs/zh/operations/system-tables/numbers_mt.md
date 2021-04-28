# system.numbers_mt {#system-numbers-mt}

与 [system.numbers](../../operations/system-tables/numbers.md) 但读取是并行的。 这些数字可以按任何顺序返回。

用于测试。

**示例**

```sql
:) SELECT * FROM system.numbers_mt LIMIT 10;
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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/numbers_mt) <!--hide-->
