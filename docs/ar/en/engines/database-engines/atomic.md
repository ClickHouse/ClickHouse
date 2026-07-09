---
description: 'يدعم المحرك `Atomic` استعلامات `DROP TABLE` و`RENAME TABLE`
  غير الحاجبة، واستعلامات `EXCHANGE TABLES` الذرية. ويُستخدم محرك قاعدة البيانات `Atomic`
  افتراضيًا.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

يدعم المحرك `Atomic` استعلامات [`DROP TABLE`](#drop-detach-table) و[`RENAME TABLE`](#rename-table) غير الحاجبة، واستعلامات [`EXCHANGE TABLES`](#exchange-tables) الذرية. ويُستخدم محرك قاعدة البيانات `Atomic` افتراضيًا في إصدار ClickHouse مفتوح المصدر.

:::note
في ClickHouse Cloud، يُستخدم [محرك قاعدة البيانات `Shared`](/ar/cloud/reference/shared-catalog#shared-database-engine) افتراضيًا، وهو يدعم أيضًا
العمليات المذكورة أعلاه.
:::

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## تفاصيل وتوصيات
</div>

<div id="table-uuid">
  ### معرّف UUID للجدول
</div>

لكل جدول في قاعدة البيانات `Atomic` [معرّف UUID](../../sql-reference/data-types/uuid.md) ثابت، وتُخزَّن بياناته في الدليل التالي:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

حيث إن `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` هو معرّف UUID الخاص بالجدول.

يُنشأ معرّف UUID تلقائيًا افتراضيًا. ومع ذلك، يمكن للمستخدمين تحديد معرّف UUID صراحةً عند إنشاء جدول، رغم أن ذلك غير موصى به.

على سبيل المثال:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
يمكنك استخدام إعداد [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) لعرض معرّف UUID في استعلام `SHOW CREATE`.
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

لا تُعدِّل استعلامات [`RENAME`](../../sql-reference/statements/rename.md) معرّف UUID ولا تنقل بيانات الجدول. وتُنفَّذ هذه الاستعلامات فورًا، من دون انتظار اكتمال الاستعلامات الأخرى التي تستخدم الجدول.

<div id="drop-detach-table">
  ### DROP/DETACH TABLE
</div>

عند استخدام `DROP TABLE`، لا تُزال أي بيانات. يكتفي المحرك `Atomic` بوضع علامة على الجدول باعتباره محذوفًا من خلال نقل metadata الخاصة به إلى `/clickhouse_path/metadata_dropped/` وإخطار الخيط الخلفي. ويُحدَّد التأخير قبل الحذف النهائي لبيانات الجدول بواسطة الإعداد [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec).
يمكنك تحديد الوضع المتزامن باستخدام المُعدِّل `SYNC`. استخدم الإعداد [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) لهذا الغرض. في هذه الحالة، ينتظر `DROP` حتى تنتهي استعلامات `SELECT` و`INSERT` وغيرها من الاستعلامات الجارية التي تستخدم الجدول. وسيُزال الجدول عندما لا يعود قيد الاستخدام.

<div id="exchange-tables">
  ### EXCHANGE TABLES/DICTIONARIES
</div>

يقوم استعلام [`EXCHANGE`](../../sql-reference/statements/exchange.md) بتبديل الجداول أو القواميس بشكل ذري. على سبيل المثال، بدلًا من هذه العملية غير الذرية:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

يمكنك استخدام قاعدة بيانات من نوع atomic:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### ReplicatedMergeTree في قاعدة بيانات atomic
</div>

بالنسبة إلى جداول [`ReplicatedMergeTree`](/ar/engines/table-engines/mergetree-family/replication)، يُنصح بعدم تحديد معلمات المحرّك الخاصة بالمسار في ZooKeeper واسم النسخة المتماثلة. في هذه الحالة، ستُستخدم معلمات الإعداد [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) و[`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name). وإذا أردت تحديد معلمات المحرّك صراحةً، فيُنصح باستخدام ماكرو `{uuid}`. وهذا يضمن إنشاء مسارات فريدة تلقائيًا لكل جدول في ZooKeeper.

<div id="metadata-disk">
  ### قرص البيانات الوصفية
</div>

عند تحديد `disk` ضمن `SETTINGS`، يُستخدَم القرص لتخزين ملفات البيانات الوصفية للجدول.
على سبيل المثال:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

إذا لم يتم تحديده، فسيُستخدم وسيط التخزين المعرّف في `database_disk.disk` افتراضيًا.

<div id="see-also">
  ## راجع أيضًا
</div>

* [system.databases](../../operations/system-tables/databases.md) جدول النظام