---
slug: /ja/sql-reference/statements/set-role
sidebar_position: 51
sidebar_label: SET ROLE
title: "SET ROLE ステートメント"
---

現在のユーザーに対してロールをアクティベートします。

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE

ユーザーにデフォルトのロールを設定します。

デフォルトのロールはユーザーのログイン時に自動的にアクティベートされます。デフォルトとして設定するには、事前に付与されたロールのみを使用できます。ユーザーにロールが付与されていない場合、ClickHouseは例外をスローします。

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

## 例

複数のデフォルトロールをユーザーに設定します:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

付与されたすべてのロールをユーザーのデフォルトとして設定します:

``` sql
SET DEFAULT ROLE ALL TO user
```

ユーザーからデフォルトロールをすべて除外します:

``` sql
SET DEFAULT ROLE NONE TO user
```

特定のロール `role1` と `role2` を除いて、付与されたすべてのロールをデフォルトとして設定します:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```
