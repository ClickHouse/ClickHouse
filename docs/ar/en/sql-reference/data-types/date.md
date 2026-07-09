---
description: 'توثيق نوع البيانات Date في ClickHouse'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

تاريخ. يُخزَّن في بايتين على شكل عدد الأيام منذ 1970-01-01 (غير موقّع). يتيح تخزين القيم بدءًا من ما بعد بداية حقبة Unix مباشرةً وحتى الحد الأعلى الذي يحدده ثابت في مرحلة الترجمة البرمجية (حاليًا، يمتد ذلك حتى عام 2149، لكن آخر عام مدعوم بالكامل هو 2148).

النطاق المدعوم للقيم: [1970-01-01, 2149-06-06].

تُخزَّن قيمة التاريخ دون المنطقة الزمنية.

**مثال**

إنشاء جدول يحتوي على عمود من النوع `Date` وإدراج البيانات فيه:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**انظر أيضًا**

* [دوال التعامل مع التاريخ والوقت](../../sql-reference/functions/date-time-functions.md)
* [عوامل التشغيل للتعامل مع التاريخ والوقت](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`DateTime` نوع البيانات](../../sql-reference/data-types/datetime.md)