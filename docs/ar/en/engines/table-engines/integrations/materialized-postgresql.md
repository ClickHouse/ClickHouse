---
description: 'ينشئ جدول ClickHouse مع تفريغ أولي لبيانات جدول PostgreSQL ويبدأ عملية
  النسخ المتماثل.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'محرك الجدول MaterializedPostgreSQL'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # محرك الجدول MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
يُوصى لمستخدمي ClickHouse Cloud باستخدام [ClickPipes](/ar/integrations/clickpipes) لنسخ PostgreSQL متماثلًا إلى ClickHouse. ويدعم ذلك أصلاً ميزة Change Data Capture ‏(CDC) عالية الأداء في PostgreSQL.
:::

ينشئ جدولًا في ClickHouse استنادًا إلى تفريغ بيانات أولي من جدول PostgreSQL، ويبدأ عملية النسخ المتماثل، أي ينفّذ مهمةً في الخلفية لتطبيق التغييرات الجديدة فور حدوثها على جدول PostgreSQL في قاعدة بيانات PostgreSQL البعيدة.

:::note
محرك الجدول هذا تجريبي. لاستخدامه، اضبط `allow_experimental_materialized_postgresql_table` على 1 في ملفات التهيئة أو باستخدام الأمر `SET`:

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

إذا كانت هناك حاجة إلى أكثر من جدول واحد، فمن المستحسن جدًا استخدام محرك قاعدة البيانات [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) بدلًا من محرك الجدول، واستخدام الإعداد `materialized_postgresql_tables_list` الذي يحدّد الجداول المطلوب إجراء النسخ المتماثل لها (وسيكون من الممكن أيضًا إضافة `schema` لقاعدة البيانات). وسيكون ذلك أفضل بكثير من حيث CPU، مع عدد أقل من الاتصالات وعدد أقل من فتحات النسخ المتماثل داخل قاعدة بيانات PostgreSQL البعيدة.

<div id="creating-a-table">
  ## إنشاء جدول
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**معلمات المحرك**

* `host:port` — عنوان خادم PostgreSQL.
* `database` — اسم قاعدة البيانات البعيدة.
* `table` — اسم الجدول البعيد.
* `user` — مستخدم PostgreSQL.
* `password` — كلمة مرور المستخدم.

<div id="requirements">
  ## المتطلبات
</div>

1. يجب أن تكون قيمة الإعداد [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) هي `logical`، وأن تكون قيمة المعلَمة `max_replication_slots` في ملف إعدادات PostgreSQL مساويةً لـ `2` على الأقل.

2. يجب أن يتضمن أي جدول يستخدم محرك `MaterializedPostgreSQL` مفتاحًا أساسيًا مطابقًا لفهرس replica identity (ويكون افتراضيًا: المفتاح الأساسي) في جدول PostgreSQL (راجع [تفاصيل فهرس replica identity](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. يُسمح فقط بقاعدة البيانات [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)).

4. لا يعمل محرك الجداول `MaterializedPostgreSQL` إلا مع إصدارات PostgreSQL &gt;= 11، لأن التنفيذ يتطلب دالة PostgreSQL ‏[pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html).

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_version` — عدّاد المعاملات. النوع: [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — علامة الحذف. النوع: [Int8](../../../sql-reference/data-types/int-uint.md). القيم الممكنة:
  * `1` — الصف غير محذوف،
  * `-1` — الصف محذوف.

لا يلزم إضافة هذه الأعمدة عند إنشاء الجدول. ويمكن الوصول إليها دائمًا في استعلام `SELECT`.
العمود `_version` يساوي موضع `LSN` في `WAL`، لذا يمكن استخدامه للتحقق من مدى حداثة النسخ المتماثل.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
لا يُدعَم نسخ قيم [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) نسخًا متماثلًا. وستُستخدَم القيمة الافتراضية لنوع البيانات.
:::