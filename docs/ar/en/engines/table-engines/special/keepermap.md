---
description: 'يتيح لك هذا المحرك استخدام عنقود Keeper/ZooKeeper كمخزن مفتاح-قيمة
  متسق، مع عمليات كتابة قابلة للاتساق الخطي وعمليات قراءة متسقة تسلسليًا.'
sidebar_label: 'KeeperMap'
sidebar_position: 150
slug: /engines/table-engines/special/keeper-map
title: 'محرك جدول KeeperMap'
doc_type: 'reference'
---

يتيح لك هذا المحرك استخدام عنقود Keeper/ZooKeeper كمخزن مفتاح-قيمة متسق، مع عمليات كتابة قابلة للاتساق الخطي وعمليات قراءة متسقة تسلسليًا.

لتمكين محرك التخزين KeeperMap، تحتاج إلى تحديد مسار ZooKeeper الذي ستُخزَّن فيه الجداول باستخدام إعداد `<keeper_map_path_prefix>`.

على سبيل المثال:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

حيث يمكن أن تكون `path` أي مسار صالح آخر في ZooKeeper.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

معلمات المحرك:

* `root_path` - مسار ZooKeeper الذي سيُخزَّن فيه `table_name`.
  يجب ألا يحتوي هذا المسار على البادئة المعرّفة في الإعداد `<keeper_map_path_prefix>`، لأن البادئة ستُضاف تلقائيًا إلى `root_path`.
  بالإضافة إلى ذلك، تكون الصيغة `auxiliary_zookeeper_cluster_name:/some/path` مدعومة أيضًا، حيث إن `auxiliary_zookeeper_cluster` هي مجموعة ZooKeeper معرّفة داخل الإعداد `<auxiliary_zookeepers>`.
  افتراضيًا، تُستخدم مجموعة ZooKeeper المعرّفة داخل الإعداد `<zookeeper>`.
* `keys_limit` - عدد المفاتيح المسموح به داخل الجدول.
  هذا حد مرن، وقد ينتهي الأمر في بعض الحالات الطرفية بوجود عدد أكبر من المفاتيح داخل الجدول.
* `primary_key_name` – أي اسم عمود في قائمة الأعمدة.
* يجب تحديد `primary key`، وهو يدعم عمودًا واحدًا فقط في المفتاح الأساسي. وسيُسلسَل المفتاح الأساسي بصيغة ثنائية باعتباره `node name` داخل ZooKeeper.
* ستُسلسَل الأعمدة الأخرى غير المفتاح الأساسي بصيغة ثنائية وفق ترتيبها المقابل، وتُخزَّن كقيمة للعقدة الناتجة التي يحددها المفتاح المُسلسَل.
* ستُحسَّن الاستعلامات التي تستخدم التصفية بالمفتاح `equals` أو `in` إلى عملية بحث عن عدة مفاتيح من `Keeper`، وإلا فسيتم جلب جميع القيم.

مثال:

```sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

مع

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

سيُخزَّن كل Value، وهو `binary serialization` لـ `(v1, v2, v3)`، داخل `/keeper_map_tables/keeper_map_table/data/serialized_key` في `Keeper`.
بالإضافة إلى ذلك، سيكون هناك soft limit يبلغ 4 لعدد المفاتيح.

إذا أُنشئت عدة جداول على مسار ZooKeeper نفسه، فستبقى القيم محفوظة ما دام هناك جدول واحد على الأقل يستخدمه.
ونتيجةً لذلك، يمكن استخدام العبارة `ON CLUSTER` عند إنشاء الجدول ومشاركة البيانات بين عدة مثيلات ClickHouse.
وبالطبع، يمكن أيضًا تشغيل `CREATE TABLE` يدويًا باستخدام المسار نفسه على مثيلات ClickHouse غير المرتبطة للحصول على التأثير نفسه لمشاركة البيانات.

<div id="supported-operations">
  ## العمليات المدعومة
</div>

<div id="inserts">
  ### عمليات insert
</div>

عند insert صفوف جديدة إلى `KeeperMap`، إذا لم يكن المفتاح موجودًا، فسيُنشأ entry جديد لهذا المفتاح.
إذا كان المفتاح موجودًا وكان الإعداد `keeper_map_strict_mode` مضبوطًا على `true`، فسيُثار استثناء، وإلا فستُستبدل القيمة الخاصة بالمفتاح.

مثال:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### الحذف
</div>

يمكن حذف الصفوف باستخدام الاستعلام `DELETE` أو `TRUNCATE`.
إذا كان المفتاح موجودًا، وكان الإعداد `keeper_map_strict_mode` مضبوطًا على `true`، فلن تنجح عملية جلب البيانات وحذفها إلا إذا أمكن تنفيذها بشكل ذري.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

<div id="updates">
  ### التحديثات
</div>

يمكن تحديث القيم باستخدام استعلام `ALTER TABLE`. لا يمكن تحديث المفتاح الأساسي.
إذا كان الإعداد `keeper_map_strict_mode` مضبوطًا على `true`، فلن ينجح جلب البيانات وتحديثها إلا إذا نُفِّذا بشكل ذري.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [إنشاء تطبيقات للتحليلات الفورية باستخدام ClickHouse وHex](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)