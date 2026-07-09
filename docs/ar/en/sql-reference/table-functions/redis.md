---
description: 'تتيح دالة الجدول هذه تكامل ClickHouse مع Redis.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

تتيح دالة الجدول هذه تكامل ClickHouse مع [Redis](https://redis.io/).

<div id="syntax">
  ## البنية
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## الوسيطات
</div>

| الوسيطة     | الوصف                                                                                                         |
| ----------- | ------------------------------------------------------------------------------------------------------------- |
| `host:port` | عنوان خادم Redis، ويمكنك تجاهل المنفذ، وسيُستخدم منفذ Redis الافتراضي 6379.                                   |
| `key`       | أي اسم عمود في قائمة الأعمدة.                                                                                 |
| `structure` | البنية الخاصة بجدول ClickHouse الذي تُرجعه هذه الدالة.                                                        |
| `db_index`  | نطاق فهرس قاعدة بيانات Redis من 0 إلى 15، والقيمة الافتراضية هي 0.                                            |
| `password`  | كلمة مرور المستخدم، والقيمة الافتراضية سلسلة فارغة.                                                           |
| `pool_size` | الحد الأقصى لحجم مجمع الاتصالات في Redis، والقيمة الافتراضية هي 16.                                           |
| `primary`   | يجب تحديده، وهو يدعم عمودًا واحدًا فقط في المفتاح الأساسي. سيُسلسل المفتاح الأساسي بصيغة ثنائية كمفتاح Redis. |

* ستُسلسل الأعمدة الأخرى بخلاف المفتاح الأساسي بصيغة ثنائية كقيمة Redis بالترتيب المقابل.
* ستُحسَّن الاستعلامات التي تتضمن تصفية على المفتاح باستخدام equals أو in إلى عمليات بحث متعددة المفاتيح من Redis. أما إذا كانت الاستعلامات من دون تصفية على المفتاح، فسيحدث فحص كامل للجدول، وهي عملية مكلفة.

[المجموعات المسماة](/ar/operations/named-collections.md) غير مدعومة لدالة الجدول `redis` في الوقت الحالي.

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن جدول، يكون فيه المفتاح هو مفتاح Redis، بينما تُجمَّع الأعمدة الأخرى معًا في قيمة Redis.

<div id="usage-example">
  ## مثال على الاستخدام
</div>

اقرأ من Redis:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

الإدراج في Redis:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## ذات صلة
</div>

* [محرك الجدول `Redis`](/ar/engines/table-engines/integrations/redis.md)
* [استخدام Redis كمصدر بيانات للقاموس](/ar/sql-reference/statements/create/dictionary/sources/redis)