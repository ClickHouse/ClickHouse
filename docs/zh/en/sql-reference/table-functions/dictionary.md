---
description: '将字典数据展示为 ClickHouse 表。其工作方式
  与 字典 引擎相同。'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

将 [字典](../statements/create/dictionary/overview.md) 数据展示为 ClickHouse 表。其工作方式与 [字典](../../engines/table-engines/special/dictionary.md) 引擎相同。

<div id="syntax">
  ## 语法
</div>

```sql
dictionary('dict')
```

<div id="arguments">
  ## 参数
</div>

* `dict` — 字典的名称。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 返回值
</div>

一个 ClickHouse 表。

<div id="examples">
  ## 示例
</div>

输入表 `dictionary_source_table`：

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

创建字典：

```sql title="Query"
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

```sql title="Query"
SELECT * FROM dictionary('new_dictionary');
```

```text title="Response"
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

<div id="related">
  ## 相关
</div>

* [字典引擎](/zh/engines/table-engines/special/dictionary)