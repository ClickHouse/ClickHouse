---
sidebar_position: 49
sidebar_label: EXCHANGE
---

# EXCHANGE语法 {#exchange}

以原子方式交换两个表或字典的名称。
此任务也可以通过使用[RENAME](./rename.md)来完成，但在这种情况下操作不是原子的。

!!! note "注意"
`EXCHANGE`仅支持[Atomic](../../engines/database-engines/atomic.md)数据库引擎.

**语法**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B
```

## EXCHANGE TABLES {#exchange_tables}

交换两个表的名称。

**语法**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B
```

## EXCHANGE DICTIONARIES {#exchange_dictionaries}

交换两个字典的名称。

**语法**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B
```

**参考**

-   [Dictionaries](../../sql-reference/dictionaries/index.md)
