---
description: 'وثائق نوع البيانات معرّف UUID في ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

المعرّف الفريد عالميًا (معرّف UUID) هو قيمة بطول 16 بايت تُستخدم لتحديد السجلات. لمزيد من المعلومات التفصيلية عن معرّفات UUID، راجع [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

مع أن هناك إصدارات مختلفة من معرّف UUID، مثل UUIDv4 وUUIDv7 (راجع [هنا](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis))، فإن ClickHouse لا يتحقق من توافق معرّفات UUID المُدرجة مع إصدار معيّن.
وتُعامَل معرّفات UUID داخليًا على أنها تسلسل من 16 بايتًا عشوائيًا، مع [تمثيل 8-4-4-4-12](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) على مستوى SQL.

مثال على قيمة معرّف UUID:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

معرّف UUID الافتراضي كله أصفار. ويُستخدم، على سبيل المثال، عند إدراج سجل جديد من دون تحديد أي قيمة لعمود معرّف UUID:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
لأسباب تاريخية، تُرتَّب معرّفات UUID بحسب نصفها الثاني.

ورغم أن ذلك مناسب لقيم UUIDv4، فقد يضعف الأداء مع الأعمدة من نوع UUIDv7 المستخدمة في تعريفات الفهرس الأساسي (أما استخدامها في مفاتيح الترتيب أو مفاتيح التقسيم فلا بأس به).
وبشكل أدق، تتكوّن قيم UUIDv7 من طابع زمني في النصف الأول وعداد في النصف الثاني.
لذلك يكون ترتيب UUIDv7 في فهارس المفاتيح الأساسية المتناثرة (أي القيم الأولى في كل index granule) بحسب حقل العداد.
وباﻻفتراض أن معرّفات UUID كانت ستُرتَّب بحسب النصف الأول (الطابع الزمني)، فمن المتوقّع أن تستبعد خطوة تحليل الفهرس الأساسي في بداية الاستعلامات جميع العلامات في كل الأجزاء ما عدا جزءًا واحدًا.
لكن عند الترتيب بحسب النصف الثاني (العداد)، فمن المتوقّع إرجاع علامة واحدة على الأقل من جميع الأجزاء، مما يؤدي إلى عمليات وصول غير ضرورية إلى القرص.
:::

مثال:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

كحلّ بديل، يمكن تحويل معرّف UUID إلى طابع زمني يُستخرج من النصف الثاني:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

النتيجة (بافتراض إدخال البيانات نفسها):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## توليد معرّفات UUID
</div>

يوفّر ClickHouse الدالة [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) لتوليد قيم UUIDv4 عشوائية.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

**مثال 1**

يوضح هذا المثال كيفية إنشاء جدول يتضمن عمودًا من نوع معرّف UUID وإدراج قيمة فيه.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**مثال 2**

في هذا المثال، لا تُحدَّد أي قيمة لعمود معرّف UUID عند إدراج السجل، أي تُدرَج قيمة معرّف UUID الافتراضية:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## القيود
</div>

لا يدعم نوع بيانات معرّف UUID سوى الدوال التي يدعمها أيضًا نوع البيانات [String](../../sql-reference/data-types/string.md) (على سبيل المثال، [min](/ar/sql-reference/aggregate-functions/reference/min) و[max](/ar/sql-reference/aggregate-functions/reference/max) و[count](/ar/sql-reference/aggregate-functions/reference/count)).

ولا يدعم نوع بيانات معرّف UUID العمليات الحسابية (على سبيل المثال، [abs](/ar/sql-reference/functions/arithmetic-functions#abs)) أو الدوال التجميعية، مثل [sum](/ar/sql-reference/aggregate-functions/reference/sum) و[avg](/ar/sql-reference/aggregate-functions/reference/avg).