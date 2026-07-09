---
description: 'يتيح هذا المحرك ربط ClickHouse مع Redis.'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'محرك جدول Redis'
doc_type: 'guide'
---

يتيح هذا المحرك ربط ClickHouse مع [Redis](https://redis.io/). ولأن Redis يعتمد نموذج المفتاح-القيمة (kv)، فإننا نوصي بشدة بقصر الاستعلامات عليه على الاستعلامات النقطية فقط، مثل `where k=xx` أو `where k in (xx, xx)`.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**معلمات المحرك**

* `host:port` — عنوان خادم Redis. يمكنك تجاهل المنفذ، وسيُستخدم منفذ Redis الافتراضي 6379.
* `db_index` — نطاق فهرس قاعدة بيانات Redis من 0 إلى 15، والقيمة الافتراضية هي 0.
* `password` — كلمة مرور المستخدم، والقيمة الافتراضية سلسلة فارغة.
* `pool_size` — الحد الأقصى لحجم تجمّع الاتصالات في Redis، والقيمة الافتراضية هي 16.
* `primary_key_name` - أي اسم عمود في قائمة الأعمدة.

:::note التسلسل
لا يدعم `PRIMARY KEY` سوى عمود واحد. سيُسلسَل المفتاح الأساسي بصيغة ثنائية كمفتاح Redis.
أما الأعمدة الأخرى غير المفتاح الأساسي، فستُسلسَل بصيغة ثنائية كقيمة Redis وفق الترتيب المقابل.
:::

يمكن أيضًا تمرير الوسائط باستخدام [المجموعات المسماة](/ar/operations/named-collections.md). في هذه الحالة، يجب تحديد `host` و`port` كلٌّ على حدة. يُوصى بهذا الأسلوب في بيئة الإنتاج. في الوقت الحالي، تكون جميع المعلمات المُمرَّرة إلى Redis باستخدام المجموعات المسماة مطلوبة.

:::note التصفية
ستُحسَّن الاستعلامات التي تتضمن `key equals` أو `in filtering` إلى عمليات جلب متعددة المفاتيح من Redis. أما إذا كانت الاستعلامات من دون مفتاح تصفية، فسيحدث فحص كامل للجدول، وهي عملية مكلفة.
:::

<div id="usage-example">
  ## مثال على الاستخدام
</div>

أنشئ جدولًا في ClickHouse باستخدام المحرّك `Redis` مع وسيطات بسيطة:

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

أو باستخدام [المجموعات المُسمّاة](/ar/operations/named-collections.md):

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

الإدراج:

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

تحديث:

لاحظ أنه لا يمكن تعديل المفتاح الأساسي.

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

حذف:

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

Truncate:

يُفرِّغ قاعدة بيانات Redis بشكل غير متزامن. ويدعم `Truncate` أيضًا وضع SYNC.

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

الربط:

الربط مع جداول أخرى.

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## القيود
</div>

يدعم Redis engine أيضًا استعلامات المسح، مثل `where k > xx`، لكن لديه بعض القيود:

1. قد يُنتج استعلام المسح بعض المفاتيح المكررة في حالات نادرة جدًا أثناء إعادة التجزئة. راجع التفاصيل في [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269).
2. أثناء المسح، قد تُنشأ مفاتيح وتُحذف، لذلك لا يمكن لمجموعة البيانات الناتجة أن تمثل لحظة زمنية صالحة.