---
description: '使用 Null 表引擎创建具有指定结构的临时表。该函数便于编写测试和进行演示。'
sidebar_label: 'null 函数'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: '参考'
---

使用 [Null](../../engines/table-engines/special/null.md) 表引擎创建具有指定结构的临时表。由于 `Null` 引擎的特性，表数据会被忽略，并且该表会在查询执行后立即被删除。该函数便于编写测试和进行演示。

<div id="syntax">
  ## 语法
</div>

```sql
null('structure')
```

<div id="argument">
  ## 参数
</div>

* `structure` — 由列及其类型组成的列表。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 返回值
</div>

具有指定结构的临时 `Null` 引擎表。

<div id="example">
  ## 示例
</div>

使用 `null` 函数的查询：

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

可以替换三个查询：

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## 相关内容
</div>

* [Null 表引擎](../../engines/table-engines/special/null.md)