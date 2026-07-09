---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: 'يعيد جدولًا بعمود `number` واحد يحتوي على تسلسل من الأعداد الصحيحة.'
doc_type: 'reference'
---

* `numbers()` – يعيد جدولًا لا نهائيًا بعمود `number` واحد (UInt64) يحتوي على أعداد صحيحة بترتيب تصاعدي، بدءًا من 0. استخدم `LIMIT` (واختياريًا `OFFSET`) لتقييد عدد الصفوف.

* `numbers(N)` – يعيد جدولًا بعمود `number` واحد (UInt64) يحتوي على أعداد صحيحة من 0 إلى `N - 1`.

* `numbers(N, M)` – يعيد جدولًا بعمود `number` واحد (UInt64) يحتوي على `M` عددًا صحيحًا من `N` إلى `N + M - 1`.

* `numbers(N, M, S)` – يعيد جدولًا بعمود `number` واحد (UInt64) يحتوي على قيم ضمن `[N, N + M)` بخطوة `S` (أي نحو `M / S` صفوف، مع التقريب للأعلى). يجب أن تكون `S` `>= 1`.

هذا مشابه لجدول النظام [`system.numbers`](/ar/operations/system-tables/numbers). ويمكن استخدامه للاختبار وتوليد قيم متتالية.

الاستعلامات التالية متكافئة:

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

الاستعلامات التالية متكافئة أيضًا:

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

الاستعلامات التالية متكافئة أيضًا:

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

<div id="examples">
  ### أمثلة
</div>

الأعداد العشرة الأولى.

```sql
SELECT * FROM numbers(10);
```

```response
 ┌─number─┐
 │      0 │
 │      1 │
 │      2 │
 │      3 │
 │      4 │
 │      5 │
 │      6 │
 │      7 │
 │      8 │
 │      9 │
 └────────┘
```

أنشئ تسلسلًا من التواريخ من 2010-01-01 إلى 2010-12-31.

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

اعثر على أول قيمة `UInt64` `>= 10^15` يكون لـ `sipHash64(number)` فيها 20 بتًا صفريًا لاحقًا.

```sql
SELECT number
FROM numbers()
WHERE number >= 1e15
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

<div id="notes">
  ### ملاحظات
</div>

* لأسباب تتعلق بالأداء، إذا كنت تعرف عدد الصفوف التي تحتاج إليها، ففضّل الصيغ المقيّدة (`numbers(N)`, `numbers(N, M[, S])`) على الصيغة غير المقيّدة `numbers()` / `system.numbers`.
* للتوليد المتوازي، استخدم `numbers_mt(...)` أو الجدول [`system.numbers_mt`](/ar/operations/system-tables/numbers_mt). لاحظ أن النتائج قد تُعاد بأي ترتيب.