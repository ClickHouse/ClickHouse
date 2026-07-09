---
description: 'PARALLEL WITH 子句文档'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'PARALLEL WITH 子句'
doc_type: 'reference'
---

可并行执行多个语句。

<div id="syntax">
  ## 语法
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

并行执行 `statement1`、`statement2`、`statement3`、... 等语句，这些语句的输出会被丢弃。

在很多情况下，并行执行语句可能比按顺序执行同样一组语句更快。例如，`statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` 很可能比 `statement1; statement2; statement3` 更快。

<div id="examples">
  ## 示例
</div>

同时创建两个表：

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

并行删除两个表：

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## 设置
</div>

设置 [max&#95;threads](../../operations/settings/settings.md#max_threads) 用于控制会创建多少个线程。

<div id="comparison-with-union">
  ## 与 UNION 的比较
</div>

`PARALLEL WITH` 子句与 [UNION](select/union.md) 有些相似，后者也会并行执行其操作数。不过，两者之间也有一些区别：

* `PARALLEL WITH` 不会返回其操作数执行的任何结果；如果其中有异常，它只会将该异常重新抛出；
* `PARALLEL WITH` 不要求其操作数具有相同的一组结果列；
* `PARALLEL WITH` 可以执行任何语句 (而不只是 `SELECT`) 。