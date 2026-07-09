---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: 'يعيد جدولًا يحتوي على عمود `prime` واحد يضم أعدادًا أولية.'
doc_type: 'reference'
---

* `primes()` – يعيد جدولًا لا نهائيًا يحتوي على عمود `prime` واحد (UInt64) يضم أعدادًا أولية بترتيب تصاعدي، بدءًا من 2. استخدم `LIMIT` (واختياريًا `OFFSET`) لتقييد عدد الصفوف.

* `primes(N)` – يعيد جدولًا يحتوي على العمود `prime` الوحيد (UInt64)، ويضم أول `N` عددًا أوليًا بدءًا من 2.

* `primes(N, M)` – يعيد جدولًا يحتوي على العمود `prime` الوحيد (UInt64)، ويضم `M` عددًا أوليًا بدءًا من العدد الأولي ذي الفهرس `N` (بفهرسة تبدأ من 0).

* `primes(N, M, S)` – يعيد جدولًا يحتوي على العمود `prime` الوحيد (UInt64)، ويضم `M` عددًا أوليًا بدءًا من العدد الأولي ذي الفهرس `N` (بفهرسة تبدأ من 0)، وبخطوة `S` حسب فهرس العدد الأولي. وتتوافق الأعداد الأولية المُعادة مع الفهارس `N, N + S, N + 2S, ..., N + (M - 1)S`. يجب أن تكون `S` `>= 1`.

هذا مشابه لجدول النظام [`system.primes`](/ar/operations/system-tables/primes).

الاستعلامات التالية متكافئة:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

تُعدّ الاستعلامات التالية مكافئة أيضًا:

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

<div id="examples">
  ### أمثلة
</div>

أول 10 أعداد أولية.

```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

أول عدد أولي أكبر من 1e15.

```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

حلّ قيدًا معياريًا على الأعداد الأولية ضمن نطاق كبير جدًا: اعثر على أول عدد أولي `p >= 10^15` بحيث يكون باقي قسمة `p` على `65537` مساويًا لـ `1`.

```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

أول 7 أعداد ميرسين الأولية.

```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

<div id="notes">
  ### ملاحظات
</div>

* أسرع الصيغ هي استعلامات النطاق المباشرة واستعلامات التصفية النقطية التي تستخدم الخطوة الافتراضية (`1`)، مثل `primes(N)` أو `primes() LIMIT N`. وتستخدم هذه الصيغ مولّدًا مُحسَّنًا للأعداد الأولية لحساب أعداد أولية كبيرة جدًا بكفاءة.
* بالنسبة إلى المصادر غير المقيّدة (`primes()` / `system.primes`)، يمكن تطبيق عوامل تصفية بسيطة على القيم، مثل `prime BETWEEN ...` و`prime IN (...)` و`prime = ...`، أثناء التوليد لتقييد نطاقات القيم التي يجري البحث فيها. على سبيل المثال، يُنفَّذ الاستعلام التالي بشكل شبه فوري:

```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```

* لا ينطبق تحسين نطاق القيم هذا على دوال الجدول المحدودة (`primes(N)`, `primes(offset, count[, step])`) مع `WHERE`، لأن هذه الصيغ تحدد جدولًا محدودًا وفق ترتيب العدد الأولي، ويجب تقييم عامل التصفية بعد توليد هذا الجدول للحفاظ على الدلالة.
* قد يكون استخدام إزاحة غير صفرية و/أو خطوة أكبر من 1 (`primes(offset, count)` / `primes(offset, count, step)`) أبطأ، لأنه قد يلزم توليد أعداد أولية إضافية وتخطيها داخليًا. إذا لم تكن بحاجة إلى إزاحة أو خطوة، فلا تُدرجهما.