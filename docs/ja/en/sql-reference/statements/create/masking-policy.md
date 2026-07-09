---
description: 'マスキングポリシーに関するドキュメント'
sidebar_label: 'マスキングポリシー'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

マスキングポリシーを作成します。これにより、特定のユーザーまたはロールがテーブルをクエリする際に、カラムの値を動的に変換またはマスキングできます。

:::tip
マスキングポリシーでは、保存されているデータを変更することなく、クエリ時に機微データを変換またはマスキングすることで、カラムレベルのデータセキュリティを実現できます。
:::

構文:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## UPDATE 句
</div>

`UPDATE` 句では、マスクするカラムとその変換方法を指定します。1 つのポリシーで複数のカラムをマスクできます。

例:

* シンプルなマスキング: `UPDATE email = '***masked***'`
* 部分マスキング: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* ハッシュベースのマスキング: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* 複数カラム: `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## WHERE 句
</div>

オプションの`WHERE`句を使用すると、行の値に応じて条件付きでマスキングを適用できます。条件に一致する行にのみマスキングが適用されます。

例:

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## TO 句
</div>

`TO` セクションでは、このポリシーをどのユーザーおよびロールに適用するかを指定します。

* `TO user1, user2`: 特定のユーザー/ロールに適用
* `TO ALL`: すべてのユーザーに適用
* `TO ALL EXCEPT user1, user2`: 指定したユーザー以外のすべてのユーザーに適用

:::note
行ポリシーとは異なり、マスキングポリシーは、そのポリシーが適用されていないユーザーには影響しません。ユーザーに適用されるマスキングポリシーがない場合は、元のデータが表示されます。
:::

<div id="priority-clause">
  ## PRIORITY 句
</div>

複数のマスキングポリシーが同一ユーザーの同一カラムを対象としている場合、`PRIORITY` 句で適用順序を決定します。ポリシーは、優先度の高いものから低いものへ順に適用されます。

デフォルトの優先度は 0 です。同じ優先度のポリシーは、適用順序が未定義です。

例:

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note パフォーマンスに関する注意事項

* マスキングポリシーは、式の複雑さによってはクエリパフォーマンスに影響を与える場合があります
* 有効なマスキングポリシーが設定されているテーブルでは、一部の最適化が無効になる場合があります
  :::