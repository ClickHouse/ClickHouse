---
description: '创建一个临时 Merge 表。表 schema 通过对底层表的列取并集并推导出通用类型得出。'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

创建一个临时 [Merge](../../engines/table-engines/special/merge.md) 表。
表 schema 通过对底层表的列取并集并推导出通用类型得出。
可用的虚拟列与 [Merge](../../engines/table-engines/special/merge.md) 表引擎中的相同。

<div id="syntax">
  ## 语法
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## 参数
</div>

| Argument        | Description                                                                                                                                                                  |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | 可能的值 (可选，默认为 `currentDatabase()`) ：<br />    - 数据库名称，<br />    - 返回数据库名称字符串的常量表达式，例如 `currentDatabase()`，<br />    - `REGEXP(expression)`，其中 `expression` 是用于匹配 DB 名称的正则表达式。 |
| `tables_regexp` | 用于匹配指定 DB 或多个 DB 中表名的正则表达式。                                                                                                                                                  |

<div id="related">
  ## 相关
</div>

* [Merge 表引擎](../../engines/table-engines/special/merge.md)