---
slug: /sql-reference/table-functions/generate_series
sidebar_position: 146
sidebar_label: 'generate_series'
title: 'generate_series (generateSeries)'
description: 'يعيد جدولًا يحتوي على العمود الوحيد `generate_series` ‏(UInt64)، ويتضمن أعدادًا صحيحة من البداية إلى النهاية شاملًا الحدّين.'
doc_type: 'مرجع'
---

الاسم البديل: `generateSeries`

<div id="syntax">
  ## الصيغة
</div>

يُرجع جدولًا يحتوي على العمود الوحيد `generate_series` (`UInt64`) ويتضمن أعدادًا صحيحة من البداية إلى النهاية، بما في ذلك القيمتان الحدّيتان:

```sql
generate_series(START, STOP)
```

يُرجع جدولًا يحتوي على العمود الوحيد `generate_series` (`UInt64`)، ويتضمن أعدادًا صحيحة من البداية إلى النهاية، شاملاً النهاية، مع تباعد بين القيم يحدده `STEP`:

```sql
generate_series(START, STOP, STEP)
```

يمكن أن تكون `STEP` قيمة سالبة، وفي هذه الحالة تُولَّد المتتالية بترتيب تنازلي من `START` نزولًا إلى `STOP`. وإذا كانت `STEP` سالبة و`START < STOP`، فستكون النتيجة فارغة.

<div id="examples">
  ## أمثلة
</div>

تعرض الاستعلامات التالية جداول بالمحتوى نفسه ولكن بأسماء أعمدة مختلفة:

```sql
SELECT * FROM numbers(10, 5);
```

```response
┌─number─┐
│     10 │
│     11 │
│     12 │
│     13 │
│     14 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 14);
```

```response
┌─generate_series─┐
│              10 │
│              11 │
│              12 │
│              13 │
│              14 │
└─────────────────┘
```

وتُرجِع الاستعلامات التالية جداول تحتوي على المحتوى نفسه ولكن بأسماء أعمدة مختلفة (مع أن الخيار الثاني أكثر كفاءة):

```sql
SELECT * FROM numbers(10, 11) WHERE number % 3 == (10 % 3);
```

```response
┌─number─┐
│     10 │
│     13 │
│     16 │
│     19 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 20, 3);
```

```response
┌─generate_series─┐
│              10 │
│              13 │
│              16 │
│              19 │
└─────────────────┘
```

أنشئ سلسلة تناقصية:

```sql
SELECT * FROM generate_series(9, 0, -1);
```

```response
┌─generate_series─┐
│               9 │
│               8 │
│               7 │
│               6 │
│               5 │
│               4 │
│               3 │
│               2 │
│               1 │
│               0 │
└─────────────────┘
```