---
description: 'محرك MongoDB هو محرك جدول للقراءة فقط يتيح قراءة البيانات من
  مجموعة بعيدة.'
sidebar_label: 'MongoDB'
sidebar_position: 135
slug: /engines/table-engines/integrations/mongodb
title: 'محرك جدول MongoDB'
doc_type: 'reference'
---

محرك MongoDB هو محرك جدول للقراءة فقط يتيح قراءة البيانات من [MongoDB](https://www.mongodb.com/) مجموعة بعيدة.

لا تتوفر سوى خوادم MongoDB بالإصدار v3.6+.
[قائمة البذور (`mongodb+srv`)](https://www.mongodb.com/docs/manual/reference/glossary/#std-term-seed-list) غير مدعومة بعد.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = MongoDB(host:port, database, collection, user, password[, options[, oid_columns]]);
```

**معلمات المحرّك**

| Parameter     | Description                                                                                                                                                                                                   |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`   | عنوان خادم MongoDB.                                                                                                                                                                                           |
| `database`    | اسم قاعدة البيانات البعيدة.                                                                                                                                                                                   |
| `collection`  | اسم المجموعة البعيدة.                                                                                                                                                                                         |
| `user`        | مستخدم MongoDB.                                                                                                                                                                                               |
| `password`    | كلمة مرور المستخدم.                                                                                                                                                                                           |
| `options`     | اختياري. [خيارات](https://www.mongodb.com/docs/manual/reference/connection-string-options/#connection-options) سلسلة الاتصال في MongoDB، على هيئة سلسلة منسّقة بصيغة URL. مثال: `'authSource=admin&ssl=true'` |
| `oid_columns` | قائمة مفصولة بفواصل بالأعمدة التي يجب التعامل معها على أنها `oid` في عبارة WHERE. وتكون `_id` افتراضيًا.                                                                                                      |

:::tip
إذا كنت تستخدم خدمة MongoDB Atlas السحابية، فيمكن الحصول على عنوان URL للاتصال من خيار &#39;Atlas SQL&#39;.
قائمة البذور(`mongodb**+srv**`) غير مدعومة بعد، ولكن ستُضاف في الإصدارات المستقبلية.
:::

بدلًا من ذلك، يمكنك تمرير URI:

```sql
ENGINE = MongoDB(uri, collection[, oid_columns]);
```

**معلمات المحرك**

| Parameter     | Description                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------------- |
| `uri`         | URI اتصال خادم MongoDB.                                                                                 |
| `collection`  | اسم المجموعة البعيدة.                                                                                   |
| `oid_columns` | قائمة مفصولة بفواصل بالأعمدة التي يجب التعامل معها على أنها `oid` في عبارة WHERE. تكون `_id` افتراضيًا. |

<div id="types-mappings">
  ## تعيينات الأنواع
</div>

| MongoDB                 | ClickHouse                                                                       |
| ----------------------- | -------------------------------------------------------------------------------- |
| bool, int32, int64      | *أي نوع رقمي باستثناء Decimals*، Boolean، String                                 |
| double                  | Float64، String                                                                  |
| date                    | Date، Date32، DateTime، DateTime64، String                                       |
| string                  | String، *أي نوع رقمي (باستثناء Decimals) إذا كان منسقًا بشكل صحيح*               |
| document                | String (بصيغة JSON)                                                              |
| array                   | Array، String (بصيغة JSON)                                                       |
| oid                     | String                                                                           |
| binary                  | String إذا كان في عمود، وسلسلة مرمّزة بصيغة base64 إذا كانت في Array أو document |
| uuid (binary subtype 4) | UUID                                                                             |
| *أي نوع آخر*            | String                                                                           |

إذا لم يُعثر على المفتاح في مستند MongoDB (على سبيل المثال، إذا لم يتطابق اسم العمود)، فستُدرج القيمة الافتراضية أو `NULL` (إذا كان العمود من النوع Nullable).

<div id="oid">
  ### OID
</div>

إذا كنت تريد التعامل مع `String` على أنه `oid` في عبارة WHERE، فما عليك سوى وضع اسم العمود في الوسيط الأخير لمحرك الجدول.
قد يكون ذلك ضروريًا عند الاستعلام عن سجل باستخدام العمود `_id`، والذي يكون نوعه `oid` افتراضيًا في MongoDB.
إذا كان الحقل `_id` في الجدول من نوع آخر، مثل `uuid`، فيجب تعيين `oid_columns` إلى قيمة فارغة، وإلا فستُستخدم القيمة الافتراضية لهذه المعلَمة، وهي `_id`.

```javascript
db.sample_oid.insertMany([
    {"another_oid_column": ObjectId()},
]);

db.sample_oid.find();
[
    {
        "_id": {"$oid": "67bf6cc44ebc466d33d42fb2"},
        "another_oid_column": {"$oid": "67bf6cc40000000000ea41b1"}
    }
]
```

افتراضيًا، لا يُتعامل إلا مع `_id` باعتباره عمود `oid`.

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid');

SELECT count() FROM sample_oid WHERE _id = '67bf6cc44ebc466d33d42fb2'; --will output 1.
SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; --will output 0
```

في هذه الحالة، ستكون النتيجة `0`، لأن ClickHouse لا يعرف أن `another_oid_column` من النوع `oid`، لذا لنصحّح ذلك:

```sql
CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('mongodb://user:pass@host/db', 'sample_oid', '_id,another_oid_column');

-- or

CREATE TABLE sample_oid
(
    _id String,
    another_oid_column String
) ENGINE = MongoDB('host', 'db', 'sample_oid', 'user', 'pass', '', '_id,another_oid_column');

SELECT count() FROM sample_oid WHERE another_oid_column = '67bf6cc40000000000ea41b1'; -- will output 1 now
```

<div id="supported-clauses">
  ## البنود المدعومة
</div>

لا تُدعم إلا الاستعلامات ذات التعبيرات البسيطة (على سبيل المثال، `WHERE field = <constant> ORDER BY field2 LIMIT <constant>`).
تُترجم هذه التعبيرات إلى لغة استعلامات MongoDB وتُنفَّذ على جانب الخادم.
يمكنك تعطيل جميع هذه القيود باستخدام [mongodb&#95;throw&#95;on&#95;unsupported&#95;query](../../../operations/settings/settings.md#mongodb_throw_on_unsupported_query).
في هذه الحالة، يحاول ClickHouse تحويل الاستعلام قدر الإمكان، لكن ذلك قد يؤدي إلى إجراء full table scan ومعالجة الاستعلام على جانب ClickHouse.

:::note
من الأفضل دائمًا تحديد نوع القيمة الحرفية صراحةً لأن Mongo يتطلب عوامل تصفية محددة النوع بدقة.
على سبيل المثال، إذا كنت تريد التصفية حسب `Date`:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'
```

لن ينجح هذا لأن Mongo لن يحوّل القيمة النصية إلى `Date` تلقائيًا، لذا عليك تحويلها يدويًا:

```sql
SELECT * FROM mongo_table WHERE date = '2024-01-01'::Date OR date = toDate('2024-01-01')
```

ينطبق ذلك على `Date` و`Date32` و`DateTime` و`Bool` و`UUID`.

:::

<div id="usage-example">
  ## مثال على الاستخدام
</div>

على افتراض تحميل مجموعة البيانات [sample&#95;mflix](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix) في MongoDB

أنشئ جدولًا في ClickHouse يتيح قراءة البيانات من مجموعة في MongoDB:

```sql title="Query"
CREATE TABLE sample_mflix_table
(
    _id String,
    title String,
    plot String,
    genres Array(String),
    directors Array(String),
    writers Array(String),
    released Date,
    imdb String,
    year String
) ENGINE = MongoDB('mongodb://<USERNAME>:<PASSWORD>@atlas-sql-6634be87cefd3876070caf96-98lxs.a.query.mongodb.net/sample_mflix?ssl=true&authSource=admin', 'movies');
```

```sql title="Query"
SELECT count() FROM sample_mflix_table
```

```text title="Response"
   ┌─count()─┐
1. │   21349 │
   └─────────┘
```

```sql title="Query"
-- JSONExtractString cannot be pushed down to MongoDB
SET mongodb_throw_on_unsupported_query = 0;

-- Find all 'Back to the Future' sequels with rating > 7.5
SELECT title, plot, genres, directors, released FROM sample_mflix_table
WHERE title IN ('Back to the Future', 'Back to the Future Part II', 'Back to the Future Part III')
    AND toFloat32(JSONExtractString(imdb, 'rating')) > 7.5
ORDER BY year
FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
title:     Back to the Future
plot:      A young man is accidentally sent 30 years into the past in a time-traveling DeLorean invented by his friend, Dr. Emmett Brown, and must make sure his high-school-age parents unite in order to save his own existence.
genres:    ['Adventure','Comedy','Sci-Fi']
directors: ['Robert Zemeckis']
released:  1985-07-03

Row 2:
──────
title:     Back to the Future Part II
plot:      After visiting 2015, Marty McFly must repeat his visit to 1955 to prevent disastrous changes to 1985... without interfering with his first trip.
genres:    ['Action','Adventure','Comedy']
directors: ['Robert Zemeckis']
released:  1989-11-22
```

```sql title="Query"
-- Find top 3 movies based on Cormac McCarthy's books
SELECT title, toFloat32(JSONExtractString(imdb, 'rating')) AS rating
FROM sample_mflix_table
WHERE arrayExists(x -> x LIKE 'Cormac McCarthy%', writers)
ORDER BY rating DESC
LIMIT 3;
```

```text title="Response"
   ┌─title──────────────────┬─rating─┐
1. │ No Country for Old Men │    8.1 │
2. │ The Sunset Limited     │    7.4 │
3. │ The Road               │    7.3 │
   └────────────────────────┴────────┘
```

<div id="troubleshooting">
  ## استكشاف الأخطاء وإصلاحها
</div>

يمكنك الاطلاع على استعلام MongoDB الذي تم إنشاؤه في السجلات على مستوى DEBUG.

يمكن العثور على تفاصيل التنفيذ في وثائق [mongocxx](https://github.com/mongodb/mongo-cxx-driver) و[mongoc](https://github.com/mongodb/mongo-c-driver).