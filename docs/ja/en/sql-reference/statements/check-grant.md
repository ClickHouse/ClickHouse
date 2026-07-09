---
description: 'CHECK GRANT に関するドキュメント'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'CHECK GRANT ステートメント'
doc_type: 'reference'
---

`CHECK GRANT` クエリは、現在のユーザー/ロールに特定の権限が付与されているかどうかを確認するために使用します。

<div id="syntax">
  ## 構文
</div>

クエリの基本構文は次のとおりです。

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — 権限の種類。

<div id="examples">
  ## 例
</div>

ユーザーにその権限が付与されている場合、レスポンス `check_grant` は `1` になります。そうでない場合、レスポンス `check_grant` は `0` になります。

`table_1.col1` が存在し、現在のユーザーに権限 `SELECT`/`SELECT(con)` またはロール (権限付き) が付与されている場合、レスポンスは `1` になります。

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

`table_2.col2` が存在しない場合、または現在のユーザーに権限 `SELECT`/`SELECT(con)` もしくはその権限を持つロールが付与されていない場合、応答は `0` です。

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## ワイルドカード
</div>

権限を指定する際には、テーブル名またはデータベース名の代わりにアスタリスク (`*`) を使用できます。ワイルドカードのルールについては、[WILDCARD GRANTS](../../sql-reference/statements/grant.md#wildcard-grants) を参照してください。