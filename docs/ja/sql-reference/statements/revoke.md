---
slug: /ja/sql-reference/statements/revoke
sidebar_position: 39
sidebar_label: REVOKE
---

# REVOKE ステートメント

ユーザーまたはロールから権限を取り消します。

## 構文

**ユーザーからの権限の取り消し**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**ユーザーからのロールの取り消し**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## 説明

権限を取り消すには、取り消したいものより広い範囲の権限を持っている場合にそれを使用できます。例えば、ユーザーが `SELECT (x,y)` 権限を持っている場合、管理者は `REVOKE SELECT(x,y) ...` や `REVOKE SELECT * ...`、さらには `REVOKE ALL PRIVILEGES ...` クエリを実行してこの権限を取り消すことができます。

### 部分的な取り消し

権限の一部を取り消すことができます。例えば、ユーザーが `SELECT *.*` 権限を持っている場合、特定のテーブルやデータベースから読み取る権限を取り消すことができます。

## 例

`john` ユーザーアカウントに、`accounts` データベースを除くすべてのデータベースの選択権限を与える:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

`mira` ユーザーアカウントに、`accounts.staff` テーブルのすべてのカラムを選択する権限を与えますが、`wage` カラムを除きます。

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[原文はこちら](https://clickhouse.com/docs/ja/operations/settings/settings/)
