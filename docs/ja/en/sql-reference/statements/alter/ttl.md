---
description: 'テーブル有効期限 (TTL)の操作に関するドキュメント'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'テーブル有効期限 (TTL)の操作'
doc_type: 'reference'
---

:::note
古いデータの管理に有効期限 (TTL)を使用する方法の詳細については、ユーザーガイドの [TTL を使用したデータ管理](/ja/guides/developer/ttl.md) を参照してください。以下では、既存の有効期限 (TTL)ルールを変更または削除する方法を説明します。
:::

<div id="modify-ttl">
  ## 有効期限 (TTL) を変更する
</div>

[テーブルの有効期限 (TTL)](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) は、次の形式のリクエストで変更できます。

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## 有効期限 (TTL) の削除
</div>

次のクエリで、テーブルから 有効期限 (TTL) プロパティを削除できます。

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**例**

テーブル `TTL` が設定された次のテーブルを考えます:

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

有効期限 (TTL) のクリーンアップを強制するには、`OPTIMIZE` を実行します:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

テーブルから2行目が削除されました。

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

次のクエリでテーブルの `TTL` を削除します。

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

削除した行を再度挿入し、`OPTIMIZE` で `TTL` のクリーンアップをもう一度強制します:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

`TTL` がなくなっているため、2 行目は削除されません。

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**関連項目**

* [有効期限 (TTL) 式](../../../sql-reference/statements/create/table.md#ttl-expression) の詳細。
* [有効期限 (TTL) を使用したカラムの変更](/ja/sql-reference/statements/alter/ttl)。