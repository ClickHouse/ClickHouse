---
description: '`EXISTS` 运算符文档'
slug: /sql-reference/operators/exists
title: 'EXISTS'
doc_type: 'reference'
---

`EXISTS` 运算符用于检查子查询结果中是否有记录。如果结果为空，则该运算符返回 `0`；否则返回 `1`。

`EXISTS` 也可用于 [WHERE](../../sql-reference/statements/select/where.md) 子句中。

:::tip
子查询中不支持引用主查询中的表和列。
:::

**语法**

```sql
EXISTS(subquery)
```

**示例**

用于检查子查询中是否存在值的查询：

```sql title="Query"
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

包含返回多行的子查询的查询：

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

带有返回空结果子查询的查询：

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```