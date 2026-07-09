---
description: 'RENAME 语句文档'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'RENAME 语句'
doc_type: 'reference'
---

用于重命名数据库、表或字典。可在单个查询中重命名多个实体。
请注意，涉及多个实体的 `RENAME` 查询属于非原子操作。若要以原子方式交换实体名称，请使用 [EXCHANGE](./exchange.md) 语句。

**语法**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

重命名数据库。

**语法**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

重命名一个或多个表。

重命名表是一项轻量级操作。如果在 `TO` 之后指定了其他数据库，表会被移动到该数据库中。不过，这些数据库所在的目录必须位于同一文件系统中。否则，将返回错误。
如果在一个查询中重命名多个表，该操作不是原子的。它可能只执行了一部分，其他会话中的查询可能会报 `Table ... does not exist ...` 错误。

**语法**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**示例**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

你也可以使用更简单的 SQL：

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

将一个或多个字典重命名。该查询还可用于在数据库之间移动字典。

**语法**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**另请参阅**

* [字典](./create/dictionary/overview.md)