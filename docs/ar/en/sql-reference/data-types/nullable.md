---
description: 'توثيق لمُعدِّل نوع البيانات Nullable في ClickHouse'
sidebar_label: 'Nullable(T)'
sidebar_position: 44
slug: /sql-reference/data-types/nullable
title: 'Nullable(T)'
doc_type: 'reference'
---

يتيح تخزين وسم خاص ([NULL](../../sql-reference/syntax.md)) يدل على &quot;قيمة مفقودة&quot; إلى جانب القيم العادية التي يسمح بها `T`. على سبيل المثال، يمكن لعمود من النوع `Nullable(Int8)` تخزين قيم من النوع `Int8`، بينما تخزّن الصفوف التي لا تحتوي على قيمة `NULL`.

لا يمكن أن يكون `T` أيًا من أنواع البيانات المركبة التالية:

* [Array](../../sql-reference/data-types/array.md) — غير مدعوم
* [Map](../../sql-reference/data-types/map.md) — غير مدعوم
* [Tuple](../../sql-reference/data-types/tuple.md) — يتوفر دعم تجريبي*

ومع ذلك، **يمكن أن تتضمن** أنواع البيانات المركبة قيمًا من النوع `Nullable`، مثل `Array(Nullable(Int8))` أو `Tuple(Nullable(String), Nullable(Int64))`.

:::note تجريبي: Nullable Tuples

* [Nullable(Tuple(...))](../../sql-reference/data-types/tuple.md#nullable-tuple) مدعوم عند تمكين `enable_nullable_tuple_type = 1`.
  :::

لا يمكن تضمين حقل من النوع `Nullable` في فهارس الجدول.

تكون `NULL` هي القيمة الافتراضية لأي نوع `Nullable`، ما لم يُحدَّد خلاف ذلك في إعدادات ClickHouse server.

<div id="storage-features">
  ## ميزات التخزين
</div>

لتخزين القيم من النوع `Nullable` في عمود من جدول، يستخدم ClickHouse ملفًا منفصلًا لأقنعة `NULL` بالإضافة إلى الملف العادي الذي يحتوي على القيم. وتتيح الإدخالات في ملف الأقنعة لـ ClickHouse التمييز بين `NULL` والقيمة الافتراضية لنوع البيانات المقابل في كل صف من صفوف الجدول. وبسبب هذا الملف الإضافي، يستهلك العمود `Nullable` مساحة تخزين إضافية مقارنةً بعمود عادي مماثل.

:::note
يكاد استخدام `Nullable` يؤثر سلبًا في الأداء دائمًا، لذا ضع ذلك في الحسبان عند تصميم قواعد بياناتك.
:::

<div id="finding-null">
  ## العثور على NULL
</div>

يمكن العثور على قيم `NULL` في عمود باستخدام العمود الفرعي `null` من دون قراءة العمود بالكامل. ويُرجع `1` إذا كانت القيمة المقابلة `NULL`، و`0` خلاف ذلك.

**مثال**

```sql title="Query"
CREATE TABLE nullable (`n` Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nullable VALUES (1) (NULL) (2) (NULL);

SELECT n.null FROM nullable;
```

```text title="Response"
┌─n.null─┐
│      0 │
│      1 │
│      0 │
│      1 │
└────────┘
```

<div id="usage-example">
  ## مثال على الاستخدام
</div>

```sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

```sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

```sql
SELECT x + y FROM t_null
```

```text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```