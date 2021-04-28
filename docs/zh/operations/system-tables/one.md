# system.one {#system-one}

此表包含一行，其中有一个 `dummy` UInt8的列包含值0。

如果 `SELECT` 查询不指定 `FROM` 子句，则使用此表。

这类似于在其他Dbms中的 `DUAL` 表。


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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/one) <!--hide-->