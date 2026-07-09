---
description: '对给定的查询字符串进行随机扰动。'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

对给定的查询字符串进行随机扰动。

<div id="syntax">
  ## 语法
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## 参数
</div>

| 参数                 | 描述                            |
| ------------------ | ----------------------------- |
| `query`            | (String) - 作为模糊测试输入的源查询。      |
| `max_query_length` | (UInt64) - 模糊测试过程中查询可达到的最大长度。 |
| `random_seed`      | (UInt64) - 用于生成可复现稳定结果的随机种子。  |

<div id="returned_value">
  ## 返回值
</div>

一个仅包含一列的表对象，该列存储经过扰动处理的查询字符串。

<div id="usage-example">
  ## 使用示例
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```