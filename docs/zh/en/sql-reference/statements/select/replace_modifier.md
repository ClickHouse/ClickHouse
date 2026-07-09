---
description: '介绍 APPLY 修饰符的文档，它允许你对查询的外层表表达式返回的每一行调用某个函数。'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'REPLACE 修饰符'
keywords: ['REPLACE', 'modifier']
doc_type: 'reference'
---

> 允许你指定一个或多个[表达式别名](/zh/sql-reference/syntax#expression-aliases)。

每个别名都必须与 `SELECT *` 语句中的某个列名相匹配。在输出列列表中，与该别名匹配的列
会被该 `REPLACE` 中的表达式替换。

此修饰符不会更改列的名称或顺序，但可以更改值及其类型。

**语法：**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**示例：**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```