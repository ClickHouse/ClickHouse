---
description: 'توثيق لنوع البيانات Date32 في ClickHouse، الذي يخزّن التواريخ
  بنطاق موسّع مقارنةً بـ Date'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'reference'
---

تاريخ. يدعم نطاق التاريخ نفسه الذي يدعمه [DateTime64](../../sql-reference/data-types/datetime64.md). يُخزَّن كعدد صحيح موقَّع من 32 بت بترتيب البايت الأصلي، وتمثّل القيمة عدد الأيام منذ `1900-01-01`. **مهم!** تمثّل القيمة 0 التاريخ `1970-01-01`، وتمثّل القيم السالبة الأيام التي تسبق `1970-01-01`.

**أمثلة**

إنشاء جدول يحتوي على عمود من النوع `Date32` وإدراج البيانات فيه:

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**راجع أيضًا**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/ar/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/ar/sql-reference/functions/type-conversion-functions#toDate32OrNull)