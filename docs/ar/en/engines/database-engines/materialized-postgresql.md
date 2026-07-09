---
description: 'ينشئ قاعدة بيانات في ClickHouse تتضمن جداول من قاعدة بيانات PostgreSQL.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
يُنصح مستخدمو ClickHouse Cloud باستخدام [ClickPipes](/ar/integrations/clickpipes) للنسخ المتماثل من PostgreSQL إلى ClickHouse. إذ يوفّر هذا دعمًا أصليًا عالي الأداء لـ Change Data Capture ‏(CDC) في PostgreSQL.
:::

ينشئ قاعدة بيانات في ClickHouse تحتوي على جداول من قاعدة بيانات PostgreSQL. في البداية، تنشئ قاعدة البيانات التي تستخدم المحرك `MaterializedPostgreSQL` لقطة snapshot لقاعدة بيانات PostgreSQL وتحمّل الجداول المطلوبة. ويمكن أن تشمل الجداول المطلوبة أي مجموعة فرعية من الجداول ضمن أي مجموعة فرعية من المخططات من قاعدة البيانات المحددة. وبالتزامن مع إنشاء اللقطة، يحصل محرك قاعدة البيانات على LSN، وبعد اكتمال التفريغ الأولي للجداول يبدأ في سحب التحديثات من WAL. بعد إنشاء قاعدة البيانات، لا تُضاف الجداول التي تُنشأ لاحقًا في قاعدة بيانات PostgreSQL تلقائيًا إلى النسخ المتماثل. ويجب إضافتها يدويًا باستخدام الاستعلام `ATTACH TABLE db.table`.

يُنفَّذ النسخ المتماثل باستخدام PostgreSQL Logical Replication Protocol، الذي لا يتيح النسخ المتماثل لـ DDL، لكنه يتيح معرفة ما إذا كانت قد حدثت تغييرات تؤدي إلى تعطيل النسخ المتماثل (مثل تغيير نوع العمود أو إضافة الأعمدة/إزالتها). وتُكتشف هذه التغييرات، وعندها تتوقف الجداول المعنية عن تلقي التحديثات. في هذه الحالة، يجب استخدام الاستعلامين `ATTACH`/ `DETACH PERMANENTLY` لإعادة تحميل الجدول بالكامل. وإذا لم يؤدِّ DDL إلى تعطيل النسخ المتماثل (على سبيل المثال، إعادة تسمية عمود)، فسيستمر الجدول في تلقي التحديثات (تتم عملية insert حسب الموضع).

:::note
محرك قاعدة البيانات هذا تجريبي. لاستخدامه، اضبط `allow_experimental_database_materialized_postgresql` على 1 في ملفات التهيئة أو باستخدام الأمر `SET`:

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## إنشاء قاعدة بيانات
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**معلمات المحرك**

* `host:port` — عنوان نقطة نهاية خادم PostgreSQL.
* `database` — اسم قاعدة بيانات PostgreSQL.
* `user` — اسم مستخدم PostgreSQL.
* `password` — كلمة مرور المستخدم.

<div id="example-of-use">
  ## مثال للاستخدام
</div>

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

<div id="dynamically-adding-table-to-replication">
  ## إضافة جداول جديدة إلى النسخ المتماثل الديناميكي
</div>

بعد إنشاء قاعدة البيانات `MaterializedPostgreSQL`، فإنها لا تكتشف تلقائيًا الجداول الجديدة في قاعدة بيانات PostgreSQL المقابلة. ويمكن إضافة هذه الجداول يدويًا:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
قبل الإصدار 22.1، كان تؤدي إضافة جدول إلى النسخ المتماثل إلى ترك فتحة النسخ المتماثل مؤقتة بدون حذف (باسم `{db_name}_ch_replication_slot_tmp`). إذا كنت تُرفق جداول في إصدار ClickHouse أقدم من 22.1، فتأكد من حذفها يدويًا (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). وإلا فسيزداد استخدام القرص. وقد أُصلحت هذه المشكلة في الإصدار 22.1.
:::

<div id="dynamically-removing-table-from-replication">
  ## إزالة الجداول من النسخ المتماثل ديناميكيًا
</div>

يمكن إزالة جداول محددة من النسخ المتماثل:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## مخططات PostgreSQL
</div>

يمكن تهيئة [المخططات](https://www.postgresql.org/docs/9.1/ddl-schemas.html) في PostgreSQL بثلاث طرق (اعتبارًا من الإصدار 21.12).

1. مخطط واحد لكل محرك قاعدة بيانات `MaterializedPostgreSQL`. ويتطلب ذلك استخدام الإعداد `materialized_postgresql_schema`.
   لا يمكن الوصول إلى الجداول إلا عبر اسم الجدول فقط:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. أي عدد من المخططات مع مجموعة محددة من الجداول ضمن محرك قاعدة بيانات `MaterializedPostgreSQL` واحد. ويتطلب ذلك استخدام الإعداد `materialized_postgresql_tables_list`. يُذكر كل جدول مع مخططه.
   يُجرى الوصول إلى الجداول باستخدام اسم المخطط واسم الجدول معًا:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

ولكن في هذه الحالة، يجب كتابة جميع الجداول في `materialized_postgresql_tables_list` مع اسم المخطط الخاص بها.
يتطلب ذلك تعيين `materialized_postgresql_tables_list_with_schema = 1`.

تحذير: في هذه الحالة، لا يُسمح بوجود نقاط في اسم الجدول.

3. أي عدد من المخططات مع مجموعة كاملة من الجداول لمحرك قاعدة بيانات `MaterializedPostgreSQL` واحد. يتطلب ذلك استخدام الإعداد `materialized_postgresql_schema_list`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

تحذير: في هذه الحالة، لا يُسمح بوجود نقاط في اسم الجدول.

<div id="requirements">
  ## المتطلبات
</div>

1. يجب ضبط إعداد [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) على القيمة `logical`، كما يجب أن تكون قيمة المعلمة `max_replication_slots` هي `2` على الأقل في ملف إعداد PostgreSQL.

2. يجب أن يحتوي كل جدول مُكرَّر على إحدى قيم [هوية النسخة](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) التالية:

* المفتاح الأساسي (افتراضيًا)

* فهرس

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

يُفحَص المفتاح الأساسي دائمًا أولًا. وإذا لم يكن موجودًا، يُفحَص بعد ذلك الفهرس المعرَّف على أنه فهرس لهوية النسخة المتماثلة.
إذا استُخدم الفهرس كهوية للنسخة المتماثلة، فيجب ألا يوجد سوى فهرس واحد من هذا النوع في الجدول.
يمكنك التحقق من النوع المستخدم لجدول معيّن باستخدام الأمر التالي:

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
لا يدعم النظام النسخ المتماثل لقيم [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html). وستُستخدم القيمة الافتراضية لنوع البيانات.
:::

<div id="settings">
  ## الإعدادات
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

يحدّد قائمة مفصولة بفواصل من جداول قاعدة بيانات PostgreSQL التي ستخضع للنسخ المتماثل عبر محرك قاعدة البيانات [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md).

يمكن أن يحتوي كل جدول على مجموعة فرعية من الأعمدة التي ستخضع للنسخ المتماثل، وتُكتب بين قوسين. وإذا أُهملت هذه المجموعة الفرعية من الأعمدة، فستخضع جميع أعمدة الجدول للنسخ المتماثل.

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

القيمة الافتراضية: قائمة فارغة — وهذا يعني أنه ستخضع قاعدة بيانات PostgreSQL بالكامل للنسخ المتماثل.

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

القيمة الافتراضية: سلسلة نصية فارغة. (يُستخدم المخطط الافتراضي)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

القيمة الافتراضية: قائمة فارغة. (يُستخدم المخطط الافتراضي.)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

يحدّد عدد الصفوف التي تُجمَع في الذاكرة قبل دفع البيانات إلى جدول قاعدة بيانات PostgreSQL.

القيم الممكنة:

* عدد صحيح موجب.

القيمة الافتراضية: `65536`.

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

فتحة للنسخ المتماثل ينشئها المستخدم. يجب استخدامها مع `materialized_postgresql_snapshot`.

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

سلسلة نصية تُحدِّد لقطة تُجرى انطلاقًا منها عملية [التفريغ الأولي لجداول PostgreSQL](../../engines/database-engines/materialized-postgresql.md). يجب استخدامها مع `materialized_postgresql_replication_slot`.

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

يمكن تغيير الإعدادات، عند الحاجة، باستخدام استعلام DDL. لكن لا يمكن تغيير الإعداد `materialized_postgresql_tables_list`. لتحديث قائمة الجداول في هذا الإعداد، استخدم استعلام `ATTACH TABLE`.

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

استخدم معرّف مستهلك فريدًا للنسخ المتماثل. القيمة الافتراضية: `0`.
إذا ضُبطت القيمة على `1`، فسيُسمح بإعداد عدة جداول `MaterializedPostgreSQL` تشير إلى جدول `PostgreSQL` نفسه.

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

يربط نوعي PostgreSQL `date` و`timestamp`/`timestamptz` بالنوعين `Date32` و`DateTime64` في ClickHouse، لأنهما يغطّيان نطاق القيم الأوسع لهذين النوعين في PostgreSQL. القيمة الافتراضية: `1`.
إذا ضُبط على `0`، فسيُستخدم بدلًا من ذلك النوعان الأضيق `Date` و`DateTime` (إذ لا يمكن تمثيل القيم الواقعة خارج نطاقهما أو القيم ذات الدقة دون الثانية).

لا يتحكم هذا الإعداد إلا في أنواع الأعمدة التي يختارها استنتاج النوع عند إنشاء الجداول المتداخلة، لذا يجب تحديده عند تنفيذ `CREATE DATABASE`. ولا يمكن تغييره بعد ذلك باستخدام `ALTER DATABASE ... MODIFY SETTING` (إذ تحتفظ الجداول المتداخلة التي أُنشئت بالفعل بأنواع أعمدتها الثابتة، ويُرفض مثل هذا التغيير)؛ أعد إنشاء قاعدة البيانات لتغييره. ولا ينطبق هذا على محرك الجدول `MaterializedPostgreSQL`، حيث تُصرَّح أنواع الأعمدة صراحةً.

<div id="notes">
  ## ملاحظات
</div>

<div id="logical-replication-slot-failover">
  ### التبديل عند الفشل لفتحة النسخ المتماثل المنطقي
</div>

فتحات النسخ المتماثل المنطقي الموجودة على الخادم الأساسي لا تكون متاحة على النسخ الاحتياطية.
لذلك، إذا حدث تبديل عند الفشل، فلن يكون الخادم الأساسي الجديد (الذي كان سابقًا الخادم الاحتياطي الفعلي) على علم بأي فتحات كانت موجودة على الخادم الأساسي القديم. وسيؤدي ذلك إلى تعطّل النسخ المتماثل من PostgreSQL.
ويتمثل أحد الحلول في إدارة فتحات النسخ المتماثل بنفسك وتحديد فتحة نسخ متماثل دائمة (يمكن العثور على بعض المعلومات [هنا](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). ستحتاج إلى تمرير اسم الفتحة عبر الإعداد `materialized_postgresql_replication_slot`، ويجب تصديرها باستخدام الخيار `EXPORT SNAPSHOT`. كما يجب تمرير مُعرّف لقطة عبر الإعداد `materialized_postgresql_snapshot`.

يرجى ملاحظة أن هذا ينبغي استخدامه فقط عند الحاجة الفعلية إليه. وإذا لم تكن هناك حاجة حقيقية لذلك أو فهم كامل لسببه، فمن الأفضل السماح لـ table engine بإنشاء فتحة النسخ المتماثل الخاصة به وإدارتها.

**مثال (من [@bchrobot](https://github.com/bchrobot))**

1. اضبط فتحة النسخ المتماثل في PostgreSQL.

   ```yaml
   apiVersion: "acid.zalan.do/v1"
   kind: postgresql
   metadata:
     name: acid-demo-cluster
   spec:
     numberOfInstances: 2
     postgresql:
       parameters:
         wal_level: logical
     patroni:
       slots:
         clickhouse_sync:
           type: logical
           database: demodb
           plugin: pgoutput
   ```

2. انتظر حتى تصبح فتحة النسخ المتماثل جاهزة، ثم ابدأ transaction وصدّر مُعرّف لقطة الخاص بها:

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. في ClickHouse، أنشئ database:

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. أنهِ transaction في PostgreSQL بمجرد تأكيد النسخ المتماثل إلى قاعدة بيانات ClickHouse. وتحقق من استمرار النسخ المتماثل بعد التبديل عند الفشل:

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### الأذونات المطلوبة
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- امتياز إنشاء query.

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- امتياز replication.

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- امتياز replication أو superuser.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- مالك الـpublication (أي `username` في محرك MaterializedPostgreSQL نفسه).

يمكن تجنب تنفيذ الأمرين `2` و`3`، وبالتالي عدم الحاجة إلى هذه الأذونات. استخدم الإعدادين `materialized_postgresql_replication_slot` و`materialized_postgresql_snapshot`. لكن بحذر شديد.

الوصول إلى الجداول التالية:

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### النسخ الاحتياطي والاستعادة
</div>

يمكن أخذ نسخة احتياطية من قاعدة بيانات `MaterializedPostgreSQL`. تُخزَّن بيانات كل جدول مُكرَّر في جدول `ReplacingMergeTree` متداخل، لذا يلتقط `BACKUP DATABASE` هذه البيانات عبر التفويض إلى الجدول المتداخل.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

لا تتوفر إمكانية استعادة قاعدة بيانات أو جدول من `MaterializedPostgreSQL` **في مكانه**. إذ يبدأ أي كائن مستعاد من `MaterializedPostgreSQL` فورًا في النسخ المتماثل من مصدر PostgreSQL المباشر، لذا فإن استعادة لقطة backup فوقه ستؤدي إلى خلط اللقطة بالحالة الحالية للمصدر البعيد. لذلك يفشل الأمر RESTORE بشكل آمن في هذه الحالة. بدلاً من ذلك، استعد البيانات الملتقطة إلى جداول `ReplacingMergeTree` عادية:

* في backup لقاعدة بيانات، يكون التعريف المخزن لكل جدول هو بالفعل جدول `ReplacingMergeTree` المتداخل الاصطناعي (وليس engine ‏`MaterializedPostgreSQL`)، لذلك يمكن استعادة كل جدول مباشرة إلى جدول جديد غير موجود بعد:

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* بالنسبة إلى backup لجدول `MaterializedPostgreSQL` مستقل، يكون التعريف المخزن هو engine ‏`MaterializedPostgreSQL` نفسه. أنشئ مسبقًا جدول `ReplacingMergeTree` له نفس البنية الخاصة بالجدول المتداخل (بما في ذلك العمودان `_sign` و`_version`) ثم استعد البيانات إليه:

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```