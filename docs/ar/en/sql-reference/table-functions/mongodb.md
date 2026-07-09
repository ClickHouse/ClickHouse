---
description: 'يتيح تنفيذ استعلامات `SELECT` على البيانات المخزنة على خادم MongoDB
  بعيد.'
sidebar_label: 'mongodb'
sidebar_position: 135
slug: /sql-reference/table-functions/mongodb
title: 'mongodb'
doc_type: 'reference'
---

يتيح تنفيذ استعلامات `SELECT` على البيانات المخزنة على خادم MongoDB بعيد.

<div id="syntax">
  ## الصيغة
</div>

```sql
mongodb(host:port, database, collection, user, password, structure[, options[, oid_columns]]);
mongodb(uri, collection, structure[, oid_columns]);
mongodb(named_collection_name[, <arg>=<value>...]);
```

<div id="arguments">
  ## الوسيطات
</div>

| الوسيطة       | الوصف                                                                                                     |
| ------------- | --------------------------------------------------------------------------------------------------------- |
| `host:port`   | عنوان خادم MongoDB.                                                                                       |
| `database`    | اسم قاعدة البيانات البعيدة.                                                                               |
| `collection`  | اسم المجموعة البعيدة.                                                                                     |
| `user`        | مستخدم MongoDB.                                                                                           |
| `password`    | كلمة مرور المستخدم.                                                                                       |
| `structure`   | مخطط جدول ClickHouse الذي تُرجعه هذه الدالة.                                                              |
| `options`     | خيارات سلسلة اتصال MongoDB (معامل اختياري).                                                               |
| `oid_columns` | قائمة مفصولة بفواصل من الأعمدة التي يجب التعامل معها كـ `oid` في عبارة WHERE. القيمة الافتراضية هي `_id`. |

:::tip
إذا كنت تستخدم خدمة MongoDB Atlas السحابية، فيُرجى إضافة هذه الخيارات:

```ini
'connectTimeoutMS=10000&ssl=true&authSource=admin'
```

:::

يمكنك أيضًا الاتصال عبر عنوان URI:

```sql
mongodb(uri, collection, structure[, oid_columns])
```

| الوسيطة       | الوصف                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------- |
| `uri`         | سلسلة الاتصال.                                                                                     |
| `collection`  | اسم المجموعة البعيدة.                                                                              |
| `structure`   | مخطط جدول ClickHouse الذي تُرجعه هذه الدالة.                                                       |
| `oid_columns` | قائمة مفصولة بفواصل للأعمدة التي يجب التعامل معها باعتبارها `oid` في عبارة WHERE. `_id` افتراضيًا. |
| :::           |                                                                                                    |

يمكنك تمرير الوسائط باستخدام مجموعة مسماة:

```sql
mongodb(_named_collection_[, host][, port][, database][, collection][, user][, password][, structure][, options][, oid_columns])
-- or
mongodb(_named_collection_[, uri][, structure][, oid_columns])
```

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن جدول له الأعمدة نفسها الموجودة في جدول MongoDB الأصلي.

<div id="examples">
  ## أمثلة
</div>

لنفترض أن لدينا مجموعة باسم `my_collection` مُعرَّفة في قاعدة بيانات MongoDB باسم `test`، وأننا نُدرِج مستندين:

```sql
db.createUser({user:"test_user",pwd:"password",roles:[{role:"readWrite",db:"test"}]})

db.createCollection("my_collection")

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.9", command: "check-cpu-usage -w 75 -c 90" }
)

db.my_collection.insertOne(
    { log_type: "event", host: "120.5.33.4", command: "system-check"}
)
```

لنُجرِ استعلامًا على المجموعة باستخدام دالة الجدول `mongodb`:

```sql
SELECT * FROM mongodb(
    '127.0.0.1:27017',
    'test',
    'my_collection',
    'test_user',
    'password',
    'log_type String, host String, command String',
    'connectTimeoutMS=10000'
)
```

أو:

```sql
SELECT * FROM mongodb(
    'mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
    'my_collection',
    'log_type String, host String, command String'
)
```

أو:

```sql
CREATE NAMED COLLECTION mongo_creds AS
       uri='mongodb://test_user:password@127.0.0.1:27017/test?connectionTimeoutMS=10000',
       collection='default_collection';

SELECT * FROM mongodb(
        mongo_creds,
        collection = 'my_collection',
        structure = 'log_type String, host String, command String'
)
```

<div id="related">
  ## مقالات ذات صلة
</div>

* [محرك الجدول `MongoDB`](/ar/engines/table-engines/integrations/mongodb.md)
* [استخدام MongoDB كمصدر للقاموس](../statements/create/dictionary/sources/mongodb.md)