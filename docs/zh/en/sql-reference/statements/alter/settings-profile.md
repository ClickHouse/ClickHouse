---
description: 'SETTINGS PROFILE 文档'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

修改 settings profile。

语法：

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` 子句允许在集群中修改 settings profile，参见 [分布式 DDL](../../../sql-reference/distributed-ddl.md)。

<div id="replacing-vs-modifying">
  ## 替换与修改设置
</div>

`ALTER SETTINGS PROFILE` 支持两种不同的方式来更改某个 profile 的设置以及其父 profile (继承的) 。两者的行为差异很大，因此务必选择正确的方式。

<div id="replacing-form">
  ### 替换形式：裸 `SETTINGS` / `INHERIT`
</div>

裸 `SETTINGS` 子句 (不带 `ADD`、`MODIFY` 或 `DROP`) 会将该 profile 的整个 settings 列表以及所有父 profile，完全替换为你明确列出的内容。凡是之前存在但未列出的内容，都会被静默删除——不会有任何警告。

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
由于裸 `SETTINGS` 形式会执行完整替换，因此，如果在一个已配置好的基础 profile 之上用它来“覆盖某一个设置”，该 profile 上的其他所有设置 (以及所有父 profile) 都会被移除。如果你只想修改单个设置，同时保留其余设置，请使用下文介绍的增量 `MODIFY`/`ADD`/`DROP` 形式。
:::

这与 [`CREATE SETTINGS PROFILE`](../create/settings-profile.md) 中 `SETTINGS` 的行为相同：该子句定义的是完整的 settings 列表。

<div id="incremental-form">
  ### 增量形式：`ADD` / `MODIFY` / `DROP`
</div>

`ADD`、`MODIFY` 和 `DROP` 关键字用于更改单个条目，同时不影响 profile 中的其他内容：

* `ADD SETTINGS variable = value [constraints]` — 添加一个尚不存在的设置。
* `MODIFY SETTINGS variable = value [constraints]` — 替换单个设置条目。整个条目 (值和约束) 都会被覆盖，因此如果你想保留 `MIN`/`MAX`/`READONLY`/等内容，需要重新指定。
* `DROP SETTINGS variable [,...]` — 删除列出的设置。
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — 添加或移除父 profile (继承的 profile) 。
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — 删除所有设置或所有父 profile。

其中多个子句可以组合在同一条语句中，例如 `DROP SETTINGS a ADD SETTINGS b = 1`。

`SET variable = value` 是 `MODIFY SETTINGS variable = value` 的别名。提供这种写法是因为 `SET` 更符合直觉，而且在本意是进行增量修改时，误写成用于整体替换的 `SETTINGS` 子句是常见错误。

<div id="examples">
  ## 示例
</div>

在保留已配置好的 profile 其余内容的同时，仅覆盖单个设置：

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

新增一个受约束设置，并删除另一个：

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

以增量方式管理父级 profile：

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

始终使用 [`SHOW CREATE SETTINGS PROFILE`](../show.md) 来验证结果：

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## 增量与全量替换
</div>

:::warning
单独使用 `SETTINGS` 子句时，在应用新设置前，**会从该 profile 中移除所有现有设置以及所有继承的 (父) profile**。
:::

如果只想修改某一项设置，同时保留其余设置，请使用 `ADD SETTINGS` 或 `MODIFY SETTINGS` (参见下方示例) 。

<div id="add-vs-modify">
  ## ADD 与 MODIFY
</div>

`ADD SETTINGS` 和 `MODIFY SETTINGS` 都会保留 profile 中的其他设置，但对于 *同一* 设置的现有项，二者的处理方式不同：

* `ADD SETTINGS variable = value ...` 会先删除 `variable` 的现有项，再插入新项。因此，它会**连同该设置的所有约束一起替换其值**。对于 `variable`，任何先前定义但你没有再次指定的 `MIN`、`MAX` 或可写性 (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) 都会被丢弃。
* `MODIFY SETTINGS variable = value ...` 会**按字段逐一合并**：它只会覆盖你实际指定的字段 (值、`MIN`、`MAX` 或可写性) ，其余字段则保持不变。

:::tip
简而言之，如果你只想微调某个设置的某一项 (例如只修改值，同时保留现有的 `MAX`) ，请使用 `MODIFY SETTINGS`；如果你想从头重新定义一个设置，请使用 `ADD SETTINGS`。
:::

<div id="examples">
  ## 示例
</div>

创建一个 profile，供下面的示例使用：

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

在保留其他设置的同时，添加或修改单个设置：

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

由于 `MODIFY` 是按字段合并的，因此仅修改某个设置项的值会保留其现有约束：

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

添加一个设置项 (同时保留其他设置项) ；如果该设置项已存在，则会将其完全重定义：

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

与 `MODIFY` 不同，如果仅使用值重新执行 `ADD`，会删除此前为该设置项定义的约束：

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

移除一个或多个已命名设置：

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

一次性删除所有设置：

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### 使用继承的 profile
</div>

在不影响 profile 自身设置的情况下，添加或移除父 profile (继承的) ：

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```