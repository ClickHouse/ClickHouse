---
slug: /ja/sql-reference/statements/exchange
sidebar_position: 49
sidebar_label: EXCHANGE
---

# EXCHANGE ステートメント

2つのテーブルまたはDictionaryの名前をアトミックに交換します。
この操作は一時的な名前を使用した[RENAME](./rename.md)クエリでも達成できますが、その場合操作はアトミックではありません。

:::note
`EXCHANGE`クエリは[Atomic](../../engines/database-engines/atomic.md)データベースエンジンでのみサポートされています。
:::

**構文**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

## EXCHANGE TABLES

2つのテーブルの名前を交換します。

**構文**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

## EXCHANGE DICTIONARIES

2つのDictionaryの名前を交換します。

**構文**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**関連項目**

- [Dictionary](../../sql-reference/dictionaries/index.md)
