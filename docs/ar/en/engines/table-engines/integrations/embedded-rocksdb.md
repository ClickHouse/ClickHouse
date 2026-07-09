---
description: 'يتيح هذا المحرك تكامل ClickHouse مع RocksDB'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'محرك الجدول EmbeddedRocksDB'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # محرك الجدول EmbeddedRocksDB
</div>

<CloudNotSupportedBadge />

يتيح هذا المحرك تكامل ClickHouse مع [RocksDB](http://rocksdb.org/).

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

معلمات المحرك:

* `ttl` - مدة صلاحية القيم. تُقبل قيمة TTL بالثواني. إذا كانت قيمة TTL تساوي 0، فسيُستخدم مثيل RocksDB عادي (من دون TTL).
* `rocksdb_dir` - المسار إلى دليل RocksDB موجود، أو مسار الوجهة لـ RocksDB الذي سيتم إنشاؤه. افتح الجدول باستخدام `rocksdb_dir` المحدد.
* `read_only` - عند ضبط `read_only` على true، يُستخدم وضع القراءة فقط. بالنسبة إلى التخزين الذي يستخدم TTL، لن يتم تشغيل compaction (لا يدويًا ولا تلقائيًا)، لذلك لن تُزال الإدخالات منتهية الصلاحية.
* `primary_key_name` – أي اسم عمود في قائمة الأعمدة.
* يجب تحديد `primary key`، وهو لا يدعم سوى عمود واحد في المفتاح الأساسي. سيُسلسَل المفتاح الأساسي بصيغة ثنائية باعتباره `rocksdb key`.
* ستُسلسَل الأعمدة الأخرى غير المفتاح الأساسي بصيغة ثنائية باعتبارها قيمة `rocksdb` وفق الترتيب المقابل.
* ستُحسَّن الاستعلامات التي تستخدم تصفية المفتاح `equals` أو `in` إلى بحث متعدد المفاتيح من `rocksdb`.

إعدادات المحرك:

* `optimize_for_bulk_insert` – يُحسَّن الجدول لعمليات الإدراج المجمّعة (سينشئ مسار الإدراج ملفات SST ويستوردها إلى قاعدة بيانات rocksdb بدلًا من الكتابة إلى memtables)؛ القيمة الافتراضية: `1`.
* `bulk_insert_block_size` - الحد الأدنى لحجم ملفات SST (من حيث الصفوف) التي تنشئها عملية الإدراج المجمّعة؛ القيمة الافتراضية: `1048449`.

مثال:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

<div id="metrics">
  ## المقاييس
</div>

يوجد أيضًا الجدول `system.rocksdb` الذي يعرض إحصاءات RocksDB:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

<div id="configuration">
  ## التهيئة
</div>

يمكنك أيضًا تغيير أي [خيارات rocksdb](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) باستخدام config:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

يكون تحسين العدّ التقريبي البسيط معطّلًا افتراضيًا، وقد يؤثر ذلك في أداء استعلامات `count()`. لتمكين هذا
التحسين، اضبط `optimize_trivial_approximate_count_query = 1`. يؤثر هذا الإعداد أيضًا في `system.tables` لمحرك EmbeddedRocksDB،
ففعِّل هذا الإعداد لرؤية القيم التقريبية لكلٍّ من `total_rows` و`total_bytes`.

<div id="supported-operations">
  ## العمليات المدعومة
</div>

<div id="inserts">
  ### عمليات الإدراج
</div>

عند إدراج صفوف جديدة في `EmbeddedRocksDB`، إذا كان المفتاح موجودًا بالفعل، فستُحدَّث القيمة، وإلا فسيُنشأ مفتاح جديد.

مثال:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### الحذف
</div>

يمكن حذف الصفوف باستخدام الاستعلام `DELETE` أو `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### التحديثات
</div>

يمكن تحديث القيم باستخدام الاستعلام `ALTER TABLE`. ولا يمكن تحديث المفتاح الأساسي.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### عمليات JOIN
</div>

يُدعَم JOIN خاص من النوع `direct` مع جداول EmbeddedRocksDB.
يتجنب هذا الـ JOIN المباشر إنشاء hash table في الذاكرة، ويصل إلى
البيانات مباشرةً من EmbeddedRocksDB.

مع عمليات JOIN الكبيرة، قد تلاحظ انخفاضًا كبيرًا في memory usage عند استخدام direct joins
لأنه لا يتم إنشاء hash table.

لتمكين direct joins:

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
عندما يُضبط `join_algorithm` على `direct, hash`، ستُستخدم عمليات الربط المباشر
متى أمكن ذلك، وإلا فستُستخدم `hash`.
:::

<div id="example">
  #### مثال
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### إنشاء جدول EmbeddedRocksDB وملؤه بالبيانات
</div>

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### أنشئ جدولًا واملأه لإجراء join مع الجدول `rdb`
</div>

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

<div id="set-the-join-algorithm-to-direct">
  ##### اضبط خوارزمية join على `direct`
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### INNER JOIN
</div>

```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```

```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

<div id="more-information-on-joins">
  ### مزيد من المعلومات عن عمليات JOIN
</div>

* [إعداد `join_algorithm`](/ar/operations/settings/settings.md#join_algorithm)
* [عبارة JOIN](/ar/sql-reference/statements/select/join.md)