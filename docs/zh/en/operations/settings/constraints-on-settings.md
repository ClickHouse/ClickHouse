---
description: '可以在 `user.xml` 配置文件的 `profiles` 部分中定义设置约束，以禁止用户通过
  `SET` 查询更改某些设置。'
sidebar_label: '设置约束'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: '设置约束'
doc_type: 'reference'
---

<div id="overview">
  ## 概述
</div>

在 ClickHouse 中，设置上的“约束”是指
可应用于设置的各种限制和规则。这些约束可用于保持
数据库的稳定性、安全性以及行为的可预测性。

<div id="defining-constraints">
  ## 定义约束
</div>

可以在 `user.xml` 配置文件的 `profiles` 部分中定义设置约束。
这些约束会禁止用户使用
[`SET`](/zh/sql-reference/statements/set) 语句更改某些设置。

约束的定义如下：

```xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
      <setting_name_6>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <disallowed>value1</disallowed>
        <disallowed>value2</disallowed>
        <disallowed>value3</disallowed>
        <changeable_in_readonly/>
      </setting_name_6>
    </constraints>
  </user_name>
</profiles>
```

如果用户尝试违反这些约束，就会抛出异常，且该设置保持不变。

<div id="types-of-constraints">
  ## 约束类型
</div>

ClickHouse 支持以下几种约束类型：

* `min`
* `max`
* `disallowed`
* `readonly` (别名 `const`)
* `changeable_in_readonly`

`min` 和 `max` 约束用于为数值设置指定上限和下限，
并且可以组合使用。

`disallowed` 约束可用于为特定设置指定一个或多个不允许的值。

`readonly` 或 `const` 约束表示用户完全无法更改相应设置。

`changeable_in_readonly` 约束类型允许用户在 `readonly` 设置为 `1` 时，
仍可在 `min`/`max` 范围内修改该设置；
否则，在 `readonly=1` 模式下不允许更改设置。

:::note
仅当启用 `settings_constraints_replace_previous` 时，才支持 `changeable_in_readonly`：

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

:::

<div id="multiple-constraint-profiles">
  ## 多个约束 profile
</div>

如果某个用户有多个生效中的 profile，则约束会被合并。
合并过程取决于 `settings_constraints_replace_previous`：

* **true** (推荐) ：相同设置的约束会在
  合并期间被替换，因此以最后一个约束为准，之前的所有约束都会被忽略。
  这也包括新约束中未设置的字段。
* **false** (默认) ：相同设置的约束会按如下方式合并：
  每类未设置的约束都会沿用前一个 profile 中的值，而每类
  已设置的约束都会替换为新 profile 中的值。

<div id="read-only">
  ## 只读模式
</div>

只读模式通过 `readonly` 设置启用，不要将其与
`readonly` 约束类型混淆：

* `readonly=0`：无只读限制。
* `readonly=1`：仅允许只读查询，且不能更改设置，
  除非设置了 `changeable_in_readonly`。
* `readonly=2`：仅允许只读查询，但可以更改设置，
  `readonly` 设置本身除外。

<div id="example-read-only">
  ### 示例
</div>

在 `users.xml` 中加入以下内容：

```xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

以下查询都会引发异常：

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
`default` profile 的处理方式较为特殊：为
`default` profile 定义的所有约束都会成为默认约束，因此会对所有用户生效，
除非为这些用户显式覆盖这些约束。
:::

<div id="constraints-on-merge-tree-settings">
  ## MergeTree 设置的约束
</div>

可以为 [MergeTree 设置](merge-tree-settings.md)设定约束。
这些约束会在创建使用 MergeTree 引擎的表时，
或修改其存储设置时生效。

在 `<constraints>` 部分中引用时，
MergeTree 设置 的名称前必须加上 `merge_tree_` 前缀。

<div id="example-read-only">
  ### 示例
</div>

您可以禁止创建显式指定 `storage_policy` 的新表

```xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```