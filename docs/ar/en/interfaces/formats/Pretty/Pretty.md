---
alias: []
description: 'توثيق تنسيق Pretty'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| الإدخال | الإخراج | اسم مستعار |
| ------- | ------- | ---------- |
| ✗       | ✔       |            |

<div id="description">
  ## الوصف
</div>

يُخرج تنسيق `Pretty` البيانات على شكل جداول مرسومة بأحرف Unicode،
باستخدام تسلسلات الهروب ANSI لعرض الألوان في الطرفية.
تُرسَم شبكة الجدول كاملة، ويشغل كل صف سطرين في الطرفية.
تُخرَج كل كتلة من النتائج كجدول منفصل.
وهذا ضروري حتى يمكن إخراج الكتل دون تخزين النتائج مؤقتًا (إذ سيكون التخزين المؤقت ضروريًا لحساب العرض المرئي لجميع القيم مسبقًا).

يُخرَج [NULL](/ar/sql-reference/syntax.md) على هيئة `ᴺᵁᴸᴸ`.

<div id="example-usage">
  ## مثال للاستخدام
</div>

مثال (لتنسيق [`PrettyCompact`](./PrettyCompact.md) كما هو موضح):

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

لا تُجرى عملية الإفلات للصفوف في أي من تنسيقات `Pretty`. يَرِد المثال التالي لتنسيق [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

لتجنّب إخراج كمية كبيرة جدًا من البيانات إلى الطرفية، لا تُطبع سوى أول `10,000` صف.
إذا كان عدد الصفوف أكبر من `10,000` أو مساويًا له، تُطبع الرسالة &quot;Showed first 10 000&quot;.

:::note
هذا التنسيق مناسب فقط لإخراج نتيجة استعلام، وليس لتحليل البيانات.
:::

يدعم تنسيق Pretty إخراج القيم الإجمالية (عند استخدام `WITH TOTALS`) والقيم القصوى (عند ضبط &#39;extremes&#39; على 1).
في هذه الحالات، تُخرَج القيم الإجمالية والقيم القصوى بعد البيانات الرئيسية، في جداول منفصلة.
يتضح ذلك في المثال التالي الذي يستخدم تنسيق [`PrettyCompact`](./PrettyCompact.md):

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## إعدادات التنسيق
</div>

<PrettyFormatSettings />