# system.one {#system-one}

此表包含一行只有一个值为 0 的 `dummy` UInt8 列的数据。

如果 `SELECT` 查询没有指定 `FROM` 子句，就会使用这个表来查询。

这个表类似于其他数据库管理系统(DMBS)中的 `DUAL` 表。

**示例**

```sql
:) SELECT * FROM system.one LIMIT 10;
```

```text
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```

[原文](https://clickhouse.com/docs/zh/operations/system-tables/one) <!--hide-->
