---
description: 'وثائق تصف المعدِّل EXCEPT الذي يحدّد أسماء عمود واحد أو أكثر لاستبعادها من النتيجة. تُحذف جميع أسماء الأعمدة المطابقة من المخرجات.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except-modifier
title: 'المعدِّل EXCEPT'
keywords: ['EXCEPT', 'modifier']
doc_type: 'reference'
---

> يحدّد أسماء عمود واحد أو أكثر لاستبعادها من النتيجة. تُحذف جميع أسماء الأعمدة المطابقة من المخرجات.

<div id="syntax">
  ## الصيغة
</div>

```sql
SELECT <expr> EXCEPT ( col_name1 [, col_name2, col_name3, ...] ) FROM [db.]table_name
```

<div id="examples">
  ## أمثلة
</div>

```sql title="Query"
SELECT * EXCEPT (i) from columns_transformers;
```

```response title="Response"
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```