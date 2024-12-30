---
slug: /ja/sql-reference/statements/create/role
sidebar_position: 40
sidebar_label: ROLE
title: "CREATE ROLE"
---

新しい[ロール](../../../guides/sre/user-management/index.md#role-management)を作成します。ロールは、[特権](../../../sql-reference/statements/grant.md#grant-privileges)の集合です。ロールに割り当てられた[ユーザー](../../../sql-reference/statements/create/user.md)は、そのロールのすべての特権を取得します。

構文:

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

## ロールの管理

ユーザーには複数のロールを割り当てることができます。ユーザーは、[SET ROLE](../../../sql-reference/statements/set-role.md)文を使用して、割り当てられたロールを任意の組み合わせで適用することができます。最終的な特権の範囲は、適用されたすべてのロールのすべての特権を組み合わせたものになります。ユーザーアカウントに直接付与された特権がある場合、それもロールによって付与された特権と組み合わされます。

ユーザーはログイン時に適用されるデフォルトのロールを持つことができます。デフォルトのロールを設定するには、[SET DEFAULT ROLE](../../../sql-reference/statements/set-role.md#set-default-role-statement)文または[ALTER USER](../../../sql-reference/statements/alter/user.md#alter-user-statement)文を使用します。

ロールを取り消すには、[REVOKE](../../../sql-reference/statements/revoke.md)文を使用します。

ロールを削除するには、[DROP ROLE](../../../sql-reference/statements/drop.md#drop-role-statement)文を使用します。削除されたロールは、割り当てられていたすべてのユーザーおよびロールから自動的に取り消されます。

## 例

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

この一連のクエリは、`db` データベースからデータを読み取る特権を持つロール `accountant` を作成します。

ユーザー `mira` にロールを割り当てるには:

``` sql
GRANT accountant TO mira;
```

ロールが割り当てられた後、ユーザーはそのロールを適用し、許可されたクエリを実行できます。例えば:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```
