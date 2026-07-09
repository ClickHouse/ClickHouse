---
description: 'توثيق لعبارة QUALIFY'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'عبارة QUALIFY'
doc_type: 'reference'
---

تسمح هذه العبارة بتصفية نتائج دوال النافذة. وهي مشابهة لعبارة [WHERE](../../../sql-reference/statements/select/where.md)، إلا أن الفرق هو أن `WHERE` يُنفَّذ قبل تقييم دوال النافذة، بينما يُنفَّذ `QUALIFY` بعد ذلك.

يمكن الإشارة في عبارة `QUALIFY` إلى نتائج دوال النافذة الواردة في عبارة `SELECT` باستخدام الاسم المستعار لها. كما يمكن لعبارة `QUALIFY` أيضًا التصفية استنادًا إلى نتائج دوال نافذة إضافية لا تظهر ضمن نتائج الاستعلام.

<div id="limitations">
  ## القيود
</div>

لا يمكن استخدام `QUALIFY` إذا لم تكن هناك دوال النافذة لتقييمها. استخدم `WHERE` بدلًا من ذلك.

<div id="examples">
  ## أمثلة
</div>

مثال:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```