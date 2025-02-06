---
slug: /ja/sql-reference/statements/drop
sidebar_position: 44
sidebar_label: DROP
---

# DROP ステートメント

既存のエンティティを削除します。`IF EXISTS` 条項が指定されている場合、エンティティが存在しないときにこれらのクエリはエラーを返しません。`SYNC` 修飾子が指定されている場合、エンティティは遅延なく削除されます。

## DROP DATABASE

`db` データベース内のすべてのテーブルを削除し、その後 `db` データベース自体を削除します。

構文:

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

## DROP TABLE

1つ以上のテーブルを削除します。

:::tip
テーブルの削除を元に戻すには、[UNDROP TABLE](/docs/ja/sql-reference/statements/undrop.md)を参照してください。
:::

構文:

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

制限事項:
- `IF EMPTY` 条項が指定されている場合、サーバーはクエリを受け取ったレプリカ上でのみテーブルの空であることを確認します。  
- 複数のテーブルを一度に削除することは、原子的な操作ではありません。つまり、あるテーブルの削除が失敗した場合、後続のテーブルは削除されません。

## DROP DICTIONARY

Dictionary を削除します。

構文:

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

## DROP USER

ユーザーを削除します。

構文:

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROLE

ロールを削除します。削除されたロールは、それが割り当てられていたすべてのエンティティから取り消されます。

構文:

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP ROW POLICY

行ポリシーを削除します。削除された行ポリシーは、それが割り当てられていたすべてのエンティティから取り消されます。

構文:

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP QUOTA

クォータを削除します。削除されたクォータは、それが割り当てられていたすべてのエンティティから取り消されます。

構文:

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP SETTINGS PROFILE

設定プロファイルを削除します。削除された設定プロファイルは、それが割り当てられていたすべてのエンティティから取り消されます。

構文:

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

## DROP VIEW

ビューを削除します。ビューは `DROP TABLE` コマンドでも削除できますが、`DROP VIEW` は `[db.]name` がビューであることを確認します。

構文:

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

## DROP FUNCTION

[CREATE FUNCTION](./create/function.md) で作成されたユーザー定義関数を削除します。システム関数は削除できません。

**構文**

``` sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**例**

``` sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

## DROP NAMED COLLECTION

名前付きコレクションを削除します。

**構文**

``` sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**例**

``` sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```
