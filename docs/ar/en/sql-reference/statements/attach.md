---
description: 'توثيق ATTACH'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'تعليمة ATTACH'
doc_type: 'مرجع'
---

تُستخدم لإرفاق جدول أو قاموس، مثلًا عند نقل قاعدة بيانات إلى خادم آخر.

**الصياغة**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

لا يُنشئ هذا الاستعلام بيانات على القرص، بل يفترض أن البيانات موجودة بالفعل في المواضع المناسبة، ويقتصر على إضافة معلومات عن الجدول أو القاموس أو قاعدة البيانات المحددة إلى الخادم. بعد تنفيذ استعلام `ATTACH`، سيصبح الخادم على علم بوجود الجدول أو القاموس أو قاعدة البيانات.

إذا كان الجدول قد فُصل سابقًا (استعلام [DETACH](../../sql-reference/statements/detach.md))، أي إن بنيته معروفة، فيمكنك استخدام الصيغة المختصرة من دون تعريف البنية.

<div id="attach-existing-table">
  ## إرفاق جدول موجود
</div>

**البنية**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

يُستخدَم هذا الاستعلام عند بدء تشغيل الخادم. يخزّن الخادم البيانات الوصفية للجداول على هيئة ملفات تحتوي على استعلامات `ATTACH`، ثم ينفّذها ببساطة عند التشغيل (باستثناء بعض جداول النظام التي تُنشأ صراحةً على الخادم).

إذا كان الجدول قد فُصل نهائيًا، فلن يُعاد إرفاقه عند بدء تشغيل الخادم، لذا تحتاج إلى استخدام استعلام `ATTACH` صراحةً.

<div id="create-new-table-and-attach-data">
  ## إنشاء جدول جديد وإرفاق البيانات به
</div>

<div id="with-specified-path-to-table-data">
  ### مع مسار محدد لبيانات الجدول
</div>

ينشئ الاستعلام جدولًا جديدًا بالبنية المحددة، ويُرفق بيانات الجدول من الدليل المحدد داخل `user_files`.

**البنية**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**مثال**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### باستخدام معرّف UUID محدد للجدول
</div>

ينشئ هذا الاستعلام جدولًا جديدًا بالبنية المحددة، ويُرفِق بيانات الجدول ذي معرّف UUID المحدد.
ويدعم محرّك قاعدة البيانات [Atomic](../../engines/database-engines/atomic.md) ذلك.

**الصيغة**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## Attach جدول MergeTree كـ ReplicatedMergeTree
</div>

يتيح ذلك إرفاق جدول MergeTree غير مكرّر كـ ReplicatedMergeTree. وسيُنشأ جدول ReplicatedMergeTree بقيم الإعدادين `default_replica_path` و`default_replica_name`. ويمكن أيضًا إرفاق جدول مكرّر كجدول MergeTree عادي.

لاحظ أن بيانات الجدول في ZooKeeper لا تتأثر بهذا الاستعلام. وهذا يعني أنه يجب عليك إضافة البيانات الوصفية في ZooKeeper باستخدام `SYSTEM RESTORE REPLICA` أو حذفها باستخدام `SYSTEM DROP REPLICA ... FROM ZKPATH ...` بعد الإرفاق.

إذا كنت تحاول إضافة replica إلى جدول ReplicatedMergeTree موجود، فضع في اعتبارك أنه سيتم فصل جميع البيانات المحلية في جدول MergeTree المُحوَّل.

**الصيغة**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**حوّل الجدول إلى Replicated**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**حوِّل الجدول إلى جدول غير مكرَّر**

احصل على مسار ZooKeeper واسم النسخة المتماثلة للجدول:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

أرفِق الجدول كجدول غير مُكرَّر، واحذف بيانات النسخة المتماثلة من ZooKeeper:

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## إرفاق قاموس موجود
</div>

يُرفق قاموسًا فُصل سابقًا.

**الصيغة**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## إرفاق قاعدة بيانات موجودة
</div>

يُرفِق قاعدة بيانات سبق فصلها.

**الصيغة**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```