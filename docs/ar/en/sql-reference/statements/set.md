---
description: 'توثيق عبارة SET'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'عبارة SET'
doc_type: 'مرجع'
---

```sql
SET param = value
```

يُعيّن `value` للإعداد `param` [setting](/ar/operations/settings/overview) للجلسة الحالية. لا يمكنك تغيير [إعدادات الخادم](../../operations/server-configuration-parameters/settings.md) بهذه الطريقة.

يمكنك أيضًا تعيين جميع القيم من ملف تعريف الإعدادات المحدد في استعلام واحد.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

بالنسبة إلى الإعدادات المنطقية المضبوطة على true، يمكنك استخدام صيغة مختصرة بحذف تعيين القيمة. وعند تحديد اسم الإعداد فقط، تُضبط قيمته تلقائيًا على `1` (true).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

يضبط المنطقة الزمنية للجلسة. هذا اسم مستعار لـ `SET session_timezone = 'timezone'`، وهو متاح للتوافق مع PostgreSQL وقواعد بيانات SQL الأخرى.

يُصدر العديد من عملاء SQL وأطر ORM وبرامج تشغيل JDBC الأمر `SET TIME ZONE` تلقائيًا عند الاتصال. تتيح هذه الصيغة لمثل هذه الأدوات العمل مع ClickHouse دون الحاجة إلى حلول بديلة مخصّصة.

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

يجب أن تكون قيمة timezone اسمًا صالحًا واردًا في [قاعدة بيانات IANA للمناطق الزمنية](https://www.iana.org/time-zones). وسيؤدي استخدام اسم timezone غير صالح إلى ظهور خطأ.

لمزيد من المعلومات حول الإعداد `session_timezone`، راجع [session&#95;timezone](/ar/operations/settings/settings#session_timezone).

<div id="setting-query-parameters">
  ## تعيين معلمات الاستعلام
</div>

يمكن أيضًا استخدام العبارة `SET` لتعريف معلمات الاستعلام بإضافة البادئة `param_` إلى اسم المعلمة.
تتيح لك معلمات الاستعلام كتابة استعلامات عامة باستخدام عناصر نائبة تُستبدل بالقيم الفعلية وقت التنفيذ.

```sql
SET param_name = value
```

لاستخدام معامل استعلام في استعلامك، أشر إليه باستخدام الصيغة `{name: datatype}`:

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

تكون معلمات الاستعلام مفيدة بشكل خاص عندما تحتاج إلى تنفيذ الاستعلام نفسه عدة مرات باستخدام قيم مختلفة.

لمزيد من المعلومات التفصيلية حول معلمات الاستعلام، بما في ذلك استخدامها مع النوع `Identifier`، راجع [تعريف معلمات الاستعلام واستخدامها](../../sql-reference/syntax.md#defining-and-using-query-parameters).

لمزيد من المعلومات، راجع [الإعدادات](../../operations/settings/settings.md).