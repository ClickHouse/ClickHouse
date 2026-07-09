---
description: 'توثيق دوال تحويل الأنواع'
sidebar_label: 'تحويل الأنواع'
slug: /sql-reference/functions/type-conversion-functions
title: 'دوال تحويل الأنواع'
doc_type: 'reference'
---

<div id="common-issues-with-data-conversion">
  ## المشكلات الشائعة في تحويل البيانات
</div>

يستخدم ClickHouse عمومًا [السلوك نفسه المتبع في برامج C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

تتعامل دوال `to<type>` و[cast](#CAST) بشكل مختلف في بعض الحالات، مثل حالة [LowCardinality](../data-types/lowcardinality.md): إذ تزيل [cast](#CAST) السمة [LowCardinality](../data-types/lowcardinality.md)، بينما لا تفعل دوال `to<type>` ذلك. وينطبق الأمر نفسه على [Nullable](../data-types/nullable.md). هذا السلوك غير متوافق مع معيار SQL، ويمكن تغييره باستخدام إعداد [cast&#95;keep&#95;nullable](../../operations/settings/settings.md/#cast_keep_nullable).

:::note
انتبه إلى احتمال فقدان البيانات إذا جرى تحويل قيم من نوع بيانات إلى نوع بيانات أصغر (على سبيل المثال من `Int64` إلى `Int32`) أو بين
أنواع بيانات غير متوافقة (على سبيل المثال من `String` إلى `Int`). واحرص على التحقق بعناية من أن النتيجة مطابقة لما هو متوقع.
:::

مثال:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

<div id="to-string-functions">
  ## ملاحظات حول دوال `toString`
</div>

تتيح عائلة الدوال `toString` التحويل بين الأرقام، والسلاسل النصية (ولكن ليس السلاسل النصية ثابتة الطول)، والتواريخ، والتواريخ التي تتضمن وقتًا.
تقبل جميع هذه الدوال وسيطة واحدة.

* عند التحويل إلى سلسلة نصية أو منها، تُنسَّق القيمة أو تُحلَّل وفقًا للقواعد نفسها المستخدمة في تنسيق TabSeparated (وفي معظم التنسيقات النصية الأخرى أيضًا). وإذا تعذر تحليل السلسلة النصية، يُطرَح استثناء ويُلغى الطلب.
* عند تحويل التواريخ إلى أرقام أو العكس، يمثّل التاريخ عدد الأيام منذ بداية حقبة Unix.
* عند تحويل التواريخ التي تتضمن وقتًا إلى أرقام أو العكس، يمثّل التاريخ والوقت عدد الثواني منذ بداية حقبة Unix.
* يمكن لدالة `toString` الخاصة بوسيطة `DateTime` أن تأخذ وسيطة `String` ثانية تتضمن اسم المنطقة الزمنية، على سبيل المثال: `Europe/Amsterdam`. وفي هذه الحالة، يُنسَّق الوقت وفقًا للمنطقة الزمنية المحددة.

<div id="to-date-and-date-time-functions">
  ## ملاحظات حول الدالتين `toDate`/`toDateTime`
</div>

تُحدَّد تنسيقات التاريخ وتنسيقات التاريخ مع الوقت للدالتين `toDate`/`toDateTime` كما يلي:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

واستثناءً من ذلك، عند التحويل من الأنواع الرقمية UInt32 أو Int32 أو UInt64 أو Int64 إلى Date، إذا كان الرقم أكبر من أو يساوي 65536، فسيُفسَّر على أنه طابع زمني Unix (وليس عدد الأيام)، ثم يُقرَّب إلى تاريخ.
وهذا يدعم الحالة الشائعة المتمثلة في كتابة `toDate(unix_timestamp)`، والتي كانت ستؤدي لولا ذلك إلى حدوث خطأ، وستستلزم كتابة الصيغة الأكثر تعقيدًا `toDate(toDateTime(unix_timestamp))`.

ويُجرى التحويل بين التاريخ وتاريخٍ مع وقت بالطريقة الطبيعية: بإضافة وقت صفري أو بحذف الوقت.

ويستخدم التحويل بين الأنواع الرقمية القواعد نفسها المستخدمة في عمليات الإسناد بين الأنواع الرقمية المختلفة في C++.

**مثال**

```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

راجع أيضًا الدالة [`toUnixTimestamp`](/ar/sql-reference/functions/date-time-functions#toUnixTimestamp).

{/* 
  يُستبدل المحتوى الداخلي للوسوم أدناه أثناء بناء إطار عمل التوثيق بـ
  وثائق مُولَّدة من system.functions. يُرجى عدم تعديل الوسوم أو إزالتها.
  راجع: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }