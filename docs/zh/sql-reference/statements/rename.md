---
sidebar_position: 48
sidebar_label: RENAME
---

# RENAME语法 {#misc_operations-rename}

重命名数据库、表或字典。 可以在单个查询中重命名多个实体。
请注意，具有多个实体的`RENAME`查询是非原子操作。 要以原子方式交换实体名称，请使用[EXCHANGE](./exchange.md)语法.

!!! note "注意"
`RENAME`仅支持[Atomic](../../engines/database-engines/atomic.md)数据库引擎.

**语法**

```sql
RENAME DATABASE|TABLE|DICTIONARY name TO new_name [,...] [ON CLUSTER cluster]
```

## RENAME DATABASE {#misc_operations-rename_database}

重命名数据库.

**语法**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

## RENAME TABLE {#misc_operations-rename_table}

重命名一个或多个表

重命名表是一个轻量级的操作。 如果在`TO`之后传递一个不同的数据库，该表将被移动到这个数据库。 但是，包含数据库的目录必须位于同一文件系统中。 否则，返回错误。
如果在一个查询中重命名多个表，则该操作不是原子操作。 可能会部分执行，其他会话中可能会得到`Table ... does not exist ...`错误。

**语法**

``` sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**示例**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

## RENAME DICTIONARY {#rename_dictionary}

重命名一个或多个词典。 此查询可用于在数据库之间移动字典。

**语法**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**参考**

-   [Dictionaries](../../sql-reference/dictionaries/index.md)
