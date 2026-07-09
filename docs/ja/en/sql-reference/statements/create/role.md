---
description: 'ロールのドキュメント'
sidebar_label: 'ロール'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

新しい[ロール](../../../guides/sre/user-management/index.md#role-management)を作成します。ロールは[権限](/ja/sql-reference/statements/grant#granting-privilege-syntax)の集合です。[ユーザー](../../../sql-reference/statements/create/user.md)にロールを割り当てると、そのロールに含まれるすべての権限が付与されます。

構文:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## ロールの管理
</div>

1 人のユーザーに複数のロールを割り当てることができます。ユーザーは、[SET ROLE](../../../sql-reference/statements/set-role.md) ステートメントを使って、割り当てられたロールを任意の組み合わせで適用できます。最終的な権限の範囲は、適用されたすべてのロールが持つ権限を組み合わせたものになります。ユーザーアカウント自体に直接付与された権限がある場合は、それらもロールによって付与された権限に加えられます。

ユーザーには、ログイン時に適用されるデフォルトロールを設定できます。デフォルトロールを設定するには、[SET DEFAULT ROLE](/ja/sql-reference/statements/set-role#set-default-role) ステートメントまたは [ALTER USER](/ja/sql-reference/statements/alter/user) ステートメントを使用します。

ロールを取り消すには、[REVOKE](../../../sql-reference/statements/revoke.md) ステートメントを使用します。

ロールを削除するには、[DROP ROLE](/ja/sql-reference/statements/drop#drop-role) ステートメントを使用します。削除されたロールは、割り当て先のすべてのユーザーおよびロールから自動的に取り消されます。

<div id="examples">
  ## 例
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

この一連のクエリで、`db`データベースのデータを読み取る権限を持つロール `accountant` を作成します。

このロールをユーザー `mira` に割り当てるには:

```sql
GRANT accountant TO mira;
```

ロールが割り当てられると、ユーザーはそのロールを有効化して、許可されたクエリを実行できます。たとえば:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```