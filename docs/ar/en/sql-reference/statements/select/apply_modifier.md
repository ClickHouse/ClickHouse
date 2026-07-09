---
description: 'وثائق تصف المُعدِّل APPLY الذي يتيح لك استدعاء دالة لكل صف تُعيده عبارة جدول خارجية في استعلام.'
sidebar_label: 'APPLY'
slug: /sql-reference/statements/select/apply-modifier
title: 'المُعدِّل APPLY'
keywords: ['APPLY', 'مُعدِّل']
doc_type: 'reference'
---

> يتيح لك استدعاء دالة لكل صف تُعيده عبارة جدول خارجية في استعلام.

<div id="syntax">
  ## الصياغة
</div>

```sql
SELECT <expr> APPLY( <func> ) FROM [db.]table_name
```

<div id="example">
  ## مثال
</div>

```sql
CREATE TABLE columns_transformers (i Int64, j Int16, k Int64) ENGINE = MergeTree ORDER by (i);
INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);
SELECT * APPLY(sum) FROM columns_transformers;
```

```response
┌─sum(i)─┬─sum(j)─┬─sum(k)─┐
│    220 │     18 │    347 │
└────────┴────────┴────────┘
```