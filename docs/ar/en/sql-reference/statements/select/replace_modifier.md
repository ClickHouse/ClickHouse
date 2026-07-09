---
description: 'توثيق يصف المُعدِّل APPLY الذي يتيح لك استدعاء دالة ما لكل صف تُرجعه عبارة جدول خارجية في استعلام.'
sidebar_label: 'REPLACE'
slug: /sql-reference/statements/select/replace-modifier
title: 'مُعدِّل Replace'
keywords: ['REPLACE', 'modifier']
doc_type: 'reference'
---

> يتيح لك تحديد اسم مستعار واحد أو أكثر من [الأسماء المستعارة للتعبيرات](/ar/sql-reference/syntax#expression-aliases).

يجب أن يطابق كل اسم مستعار اسمَ عمود من عبارة `SELECT *`. وفي قائمة أعمدة المخرجات، يُستبدل العمود الذي يطابق
الاسم المستعار بالتعبير الموجود في `REPLACE` ذلك.

لا يغيّر هذا المُعدِّل أسماء الأعمدة أو ترتيبها. ومع ذلك، يمكنه تغيير القيمة ونوعها.

**الصيغة:**

```sql
SELECT <expr> REPLACE( <expr> AS col_name) from [db.]table_name
```

**مثال:**

```sql
SELECT * REPLACE(i + 1 AS i) from columns_transformers;
```

```response
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```