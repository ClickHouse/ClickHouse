---
slug: /ja/sql-reference/statements/alter/ttl
sidebar_position: 44
sidebar_label: TTL
---

# テーブルのTTL操作

:::note
古いデータを管理するためのTTLの使用に関する詳細をお探しの場合は、[TTLでデータを管理する](/docs/ja/guides/developer/ttl.md)ユーザーガイドを参照してください。以下のドキュメントでは、既存のTTLルールを変更または削除する方法を示しています。
:::

## MODIFY TTL

[テーブルTTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)を以下の形式のリクエストで変更できます:

``` sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

## REMOVE TTL

テーブルからTTLプロパティを削除するには、以下のクエリを使用します:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**例**

テーブル`TTL`がある場合:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

`OPTIMIZE`を実行して`TTL`のクリーンアップを強制する:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
2行目はテーブルから削除されました。

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

以下のクエリでテーブル`TTL`を削除します:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

削除された行を再挿入し、`OPTIMIZE`で再度`TTL`のクリーンアップを強制します:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

`TTL`はもう存在しないため、2行目は削除されません:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**関連項目**

- [TTL式](../../../sql-reference/statements/create/table.md#ttl-expression)の詳細。
- [TTL付き](../../../sql-reference/statements/alter/column.md#alter_modify-column)でカラムを修正。
