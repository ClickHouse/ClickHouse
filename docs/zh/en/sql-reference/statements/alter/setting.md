---
description: '表设置修改文档'
sidebar_label: 'SETTING'
sidebar_position: 38
slug: /sql-reference/statements/alter/setting
title: '表设置修改'
doc_type: 'reference'
---

提供了一组用于修改表设置的查询。您可以修改设置，也可以将其重置为默认值。单条查询可同时更改多个设置。
如果不存在指定名称的设置，则该查询会引发异常。

**语法**

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY|RESET SETTING ...
```

:::note
这些查询仅适用于 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) 表。
:::

<div id="modify-setting">
  ## MODIFY SETTING
</div>

修改表设置。

**语法**

```sql
MODIFY SETTING setting_name=value [, ...]
```

**示例**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id;

ALTER TABLE example_table MODIFY SETTING max_part_loading_threads=8, max_parts_in_total=50000;
```

<div id="reset-setting">
  ## RESET SETTING
</div>

将表设置重置为默认值。如果某项设置已处于默认状态，则不会执行任何操作。

**语法**

```sql
RESET SETTING setting_name [, ...]
```

**示例**

```sql
CREATE TABLE example_table (id UInt32, data String) ENGINE=MergeTree() ORDER BY id
    SETTINGS max_part_loading_threads=8;

ALTER TABLE example_table RESET SETTING max_part_loading_threads;
```

**另请参阅**

* [MergeTree 设置](../../../operations/settings/merge-tree-settings.md)