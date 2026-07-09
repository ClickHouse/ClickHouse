---
description: '数据脱敏策略文档'
sidebar_label: '数据脱敏策略'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE 数据脱敏策略'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

创建数据脱敏策略，以便在特定用户或角色查询表时，动态转换或屏蔽列值。

:::tip
数据脱敏策略可在不修改已存储数据的情况下，于查询时对敏感数据进行转换，从而提供列级数据安全保护。
:::

语法：

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## UPDATE 子句
</div>

`UPDATE` 子句用于指定需要脱敏的列以及转换方式。你可以在单个策略中对多个列进行脱敏。

示例：

* 简单脱敏：`UPDATE email = '***masked***'`
* 部分脱敏：`UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* 基于哈希的脱敏：`UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* 多列脱敏：`UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## `WHERE` 子句
</div>

可选的 `WHERE` 子句支持根据行中的值按条件进行脱敏处理。只有符合该条件的行才会应用脱敏。

示例：

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## TO 子句
</div>

在 `TO` 部分中，指定该策略适用于哪些用户和角色。

* `TO user1, user2`：适用于特定用户/角色
* `TO ALL`：适用于所有用户
* `TO ALL EXCEPT user1, user2`：适用于除指定用户外的所有用户

:::note
与行策略不同，脱敏策略不会影响未被应用该策略的用户。如果某个用户不适用任何脱敏策略，他们将看到原始数据。
:::

<div id="priority-clause">
  ## PRIORITY 子句
</div>

当多个脱敏策略针对同一用户的同一列时，`PRIORITY` 子句用于确定其应用顺序。策略会按照优先级从高到低依次应用。

默认优先级为 0。优先级相同的策略，其应用顺序未定义。

示例：

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

:::note 性能注意事项

* 脱敏策略可能会因表达式复杂度而影响查询性能
* 对于启用了脱敏策略的表，某些优化可能会被禁用
  :::