---
description: 'DROP文のドキュメント'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'DROP文'
doc_type: 'リファレンス'
---

既存のエンティティを削除します。`IF EXISTS` 句を指定すると、エンティティが存在しない場合でもエラーは返されません。`SYNC` 修飾子を指定すると、エンティティは遅延なく削除されます。

<div id="drop-database">
  ## DROP DATABASE
</div>

`db`データベース内のすべてのテーブルを削除してから、`db`データベース自体を削除します。

構文:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

1つ以上のテーブルを削除します。

:::tip
テーブルの削除を元に戻すには、[UNDROP TABLE](/ja/sql-reference/statements/undrop.md)を参照してください。
:::

構文:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

制限事項:

* `IF EMPTY` 句を指定した場合、サーバーがテーブルの空状態を確認するのは、クエリを受け取ったレプリカ上のみです。
* 複数のテーブルを一度に削除する処理はアトミックではありません。つまり、あるテーブルの削除に失敗した場合、それ以降のテーブルは削除されません。

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

Dictionaryを削除します。

構文:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

ユーザーを削除します。

構文:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

ロールを削除します。削除されたロールは、割り当て先のすべてのエンティティから取り消されます。

構文:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

ROW POLICY を削除します。削除した ROW POLICY は、割り当て先のすべてのエンティティから取り消されます。

構文:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

マスキングポリシーを削除します。

構文:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

QUOTA を削除します。削除された QUOTA は、割り当て先のすべてのエンティティから取り消されます。

構文:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

設定プロファイルを削除します。削除された設定プロファイルは、割り当て先のすべてのエンティティから取り消されます。

構文:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

ビューを削除します。ビューは `DROP TABLE` コマンドでも削除できますが、`DROP VIEW` では `[db.]name` がビューであることを確認します。

構文:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

[CREATE FUNCTION](./create/function.md) で作成したユーザー定義関数を削除します。
システム関数は削除できません。

**構文**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**例**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

名前付きコレクションを削除します。

**構文**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**例**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```