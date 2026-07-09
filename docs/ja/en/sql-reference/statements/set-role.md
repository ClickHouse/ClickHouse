---
description: 'SET ROLE に関するドキュメント'
sidebar_label: 'SET ROLE'
sidebar_position: 51
slug: /sql-reference/statements/set-role
title: 'SET ROLE ステートメント'
doc_type: 'reference'
---

現在のユーザーのロールを有効にします。

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

<div id="set-default-role">
  ## SET DEFAULT ROLE
</div>

ユーザーのデフォルトロールを設定します。

デフォルトロールは、ユーザーのログイン時に自動的に有効になります。デフォルトとして設定できるのは、事前に付与されたロールのみです。ロールがユーザーに付与されていない場合、ClickHouse は例外をスローします。

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

<div id="examples">
  ## 例
</div>

ユーザーに複数のデフォルトロールを設定する:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

ユーザーに付与済みのすべてのロールをデフォルトに設定します:

```sql
SET DEFAULT ROLE ALL TO user
```

ユーザーからデフォルトロールを削除する:

```sql
SET DEFAULT ROLE NONE TO user
```

特定のロール `role1` と `role2` を除き、付与されているすべてのロールをデフォルトロールとして設定します:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```