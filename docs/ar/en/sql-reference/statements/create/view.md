---
description: 'مرجع CREATE VIEW'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

ينشئ عرضًا جديدًا. يمكن أن تكون العروض [عادية](#normal-view) أو [مُجسَّدة](#materialized-view) أو [مُجسَّدة قابلة للتحديث](#refreshable-materialized-view) أو [نافذة](/ar/sql-reference/statements/create/view#window-view).

<div id="normal-view">
  ## العرض العادي
</div>

الصيغة:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

لا تخزّن العروض العادية أي بيانات. فهي تكتفي بالقراءة من جدول آخر عند كل عملية وصول إليها. وبعبارة أخرى، فإن العرض العادي ليس سوى استعلام محفوظ. وعند القراءة من عرض، يُستخدم هذا الاستعلام المحفوظ بوصفه استعلامًا فرعيًا في عبارة [FROM](../../../sql-reference/statements/select/from.md).

على سبيل المثال، لنفترض أنك أنشأت عرض:

```sql
CREATE VIEW view AS SELECT ...
```

وقمت بكتابة استعلام:

```sql
SELECT a, b, c FROM view
```

هذا الاستعلام مطابق تمامًا لاستخدام الاستعلام الفرعي:

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## العرض ذو المعلمات
</div>

تشبه العروض ذات المعلمات العروض العادية، لكنها تُنشأ بمعلمات لا تُحدَّد قيمها فورًا. ويمكن استخدام هذه العروض مع دوال الجداول، بحيث يُحدَّد اسم العرض كاسمٍ للدالة وتُمرَّر قيم المعلمات بوصفها وسائطها.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

ينشئ ما سبق عرضًا للجدول، ويمكن استخدامها كدالة جدول بعد استبدال المعلمات كما هو موضح أدناه.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## عرض مادي
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` و`IF NOT EXISTS` لا يمكن استخدامهما معًا: فالجمع بينهما يُعدّ خطأً في الصياغة.

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

يستبدل `CREATE OR REPLACE MATERIALIZED VIEW` استبدالًا ذريًا عرضًا ماديًا موجودًا وجدول التخزين الداخلي الخاص به (إن وُجد). وتتطلب هذه العملية محرك قاعدة بيانات من نوع `Atomic` أو `Replicated`.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

السلوكيات الأساسية:

* **من دون عبارة `TO`**: يُحذف الجدول الداخلي القديم ويُنشأ جدول جديد. وتُفقد البيانات الموجودة في الجدول الداخلي ما لم يتم تحديد `POPULATE`.
* **مع عبارة `TO`**: يُستبدل تعريف العرض فقط؛ ولا يتأثر الجدول الهدف ولا بياناته.
* متوافق مع `REFRESH` و`ON CLUSTER` وجميع خيارات الـ engine. ولا يكون `POPULATE` مدعومًا إلا في قواعد البيانات `Atomic` — ويُرفض في قواعد البيانات `Replicated` (راجع ملاحظة `POPULATE` أدناه).
* يتطلب امتيازات `CREATE VIEW` و`DROP VIEW`.

:::note
لا يكون `CREATE OR REPLACE MATERIALIZED VIEW` مدعومًا إلا مع محركات قواعد البيانات `Atomic` أو `Replicated`. وهو غير مدعوم مع محرك قاعدة البيانات `Ordinary`.
:::

**أمثلة:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
إليك دليلًا تفصيليًا لاستخدام [العروض المادية](/ar/guides/developer/cascading-materialized-views.md).
:::

تخزّن العروض المادية البيانات التي يحوّلها استعلام [SELECT](../../../sql-reference/statements/select/index.md) المقابل.

عند إنشاء عرض مادي بدون `TO [db].[table]`، يجب تحديد `ENGINE`، وهو محرك الجدول المستخدم لتخزين البيانات.

عند إنشاء عرض مادي باستخدام `TO [db].[table]`، لا يمكنك أيضًا استخدام `POPULATE`.

يُنفَّذ العرض المادي على النحو التالي: عند إدراج بيانات في الجدول المحدد في `SELECT`، يحوّل استعلام `SELECT` هذا جزءًا من البيانات المُدرجة، ثم تُدرَج النتيجة في العرض.

:::note
تستخدم العروض المادية في ClickHouse **أسماء الأعمدة** بدلًا من ترتيب الأعمدة عند الإدراج في جدول الوجهة. وإذا لم تكن بعض أسماء الأعمدة موجودة في نتيجة استعلام `SELECT`، فسيستخدم ClickHouse قيمة افتراضية، حتى إذا لم يكن العمود من النوع [Nullable](../../data-types/nullable.md). ومن الممارسات الآمنة إضافة أسماء مستعارة لكل عمود عند استخدام العروض المادية.

تعمل العروض المادية في ClickHouse بصورة أقرب إلى مشغلات الإدراج. وإذا كان استعلام العرض يتضمن أي تجميع، فسيُطبَّق فقط على دفعة البيانات المُدرجة حديثًا. وأي تغييرات على البيانات الحالية في الجدول المصدر (مثل update أو delete أو drop partition وغير ذلك) لا تغيّر العرض المادي.

لا تتمتع العروض المادية في ClickHouse بسلوك حتمي في حالة الأخطاء. وهذا يعني أن الكتل التي كُتبت بالفعل ستبقى محفوظة في جدول الوجهة، لكن جميع الكتل التي تلي الخطأ لن تُحفَظ.

افتراضيًا، إذا أدى الدفع إلى أحد العروض إلى حدوث استثناء، فسيفشل استعلام `INSERT`. ولا يوجد ما يضمن ما إذا كانت الكتلة قد وصلت بالفعل إلى الجدول المصدر عند تلك النقطة، إذ يعتمد ذلك على توقيت مسار الإدراج، لا على خطأ العرض. أعد محاولة تنفيذ `INSERT` الفاشل باستخدام insert deduplication (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`) للحصول على exactly-once delivery إلى الجدول المصدر وجميع العروض التابعة.

إن تعيين `materialized_views_ignore_errors=true` في استعلام `INSERT` لا يغيّر سوى طريقة الإبلاغ عن الأخطاء: يُسجَّل كل خطأ في العرض كتحذير، وينجح استعلام `INSERT`. ويكون التسليم إلى الوجهة الخاصة بالعرض الذي فشل جزئيًا — إذ تُحفَظ الكتل التي عولجت قبل الاستثناء، بينما تُسقَط الكتلة الفاشلة وأي كتل لاحقة من ذلك العرض. أما العروض التابعة لتلك الوجهة فلا ترى إلا الكتل التي وصلت فعلًا، لذلك يكون تسليمها جزئيًا أيضًا. في المقابل، تُكتَب البيانات بالكامل إلى العروض الشقيقة (وسلاسلها التابعة) التي لم تُطلق استثناءً، كما تُكتَب إلى الجدول المصدر كالمعتاد. وبما أن `INSERT` يُبلّغ عن النجاح، فلن يتلقى العميل أي إشارة إلى الفشل، ولن تتم أي إعادة محاولة تلقائيًا؛ لذا استخدم هذا الإعداد فقط عندما يجب ألّا تُحجَب عمليات الكتابة إلى الجدول المصدر بسبب مشكلات في جهة العرض (على سبيل المثال، جداول `system.*_log`).

تكون `materialized_views_ignore_errors` مضبوطة على `true` افتراضيًا لجداول `system.*_log`.
:::

إذا حدّدت `POPULATE`، فستُدرَج بيانات الجدول الموجودة مسبقًا في العرض عند إنشائه، كما لو أنك تنفّذ `CREATE TABLE ... AS SELECT ...`. بخلاف ذلك، لن يحتوي الاستعلام إلا على البيانات المُدرجة في الجدول بعد إنشاء العرض. نحن **لا نوصي** باستخدام `POPULATE`، لأن البيانات المُدرجة في الجدول أثناء إنشاء العرض لن تُدرَج فيه.

:::note
نظرًا لأن `POPULATE` يعمل مثل `CREATE TABLE ... AS SELECT ...`، فله القيود التالية:

* غير مدعوم مع Replicated database
* غير مدعوم في ClickHouse Cloud

بدلًا من ذلك، يمكن استخدام `INSERT ... SELECT` منفصل.
:::

يمكن أن يحتوي استعلام `SELECT` على `DISTINCT` و`GROUP BY` و`ORDER BY` و`LIMIT`. لاحظ أن التحويلات المقابلة تُنفَّذ بشكل مستقل على كل كتلة من البيانات المُدرجة. على سبيل المثال، إذا تم تعيين `GROUP BY`، فتُجمَّع البيانات أثناء الإدراج، ولكن فقط ضمن حزمة واحدة من البيانات المُدرجة. ولن تُجمَّع البيانات لاحقًا. والاستثناء هو عند استخدام `ENGINE` ينفّذ تجميع البيانات بشكل مستقل، مثل `SummingMergeTree`.

إذا كان العرض المادي يستخدم البنية `TO [db.]name`، فيمكنك `DETACH` العرض، ثم تشغيل `ALTER` على الجدول الهدف، ثم `ATTACH` العرض الذي سبق فصله (`DETACH`).

لاحظ أن العرض المادي يتأثر بإعداد [optimize&#95;on&#95;insert](/ar/operations/settings/settings#optimize_on_insert). تُدمَج البيانات قبل إدراجها في العرض.

تبدو العروض مثل الجداول العادية تمامًا. على سبيل المثال، تُدرَج في نتيجة استعلام `SHOW TABLES`.

لحذف عرض، استخدم [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). رغم أن `DROP TABLE` يعمل أيضًا مع VIEWs.

<div id="sql_security">
  ## أمان SQL
</div>

يتيح لك `DEFINER` و`SQL SECURITY` تحديد مستخدم ClickHouse الذي سيُستخدم عند تنفيذ الاستعلام الأساسي للعرض.
تتضمن `SQL SECURITY` ثلاث قيم مسموح بها: `DEFINER` أو `INVOKER` أو `NONE`. ويمكنك تحديد أي مستخدم موجود أو `CURRENT_USER` في عبارة `DEFINER`.

يوضح الجدول التالي الصلاحيات المطلوبة من كل مستخدم لكي يتمكن من إجراء `SELECT` من العرض.
لاحظ أنه بغض النظر عن خيار أمان SQL، يظل من المطلوب في جميع الحالات وجود `GRANT SELECT ON <view>` للقراءة منه.

| خيار أمان SQL   | العرض                                                          | العرض المادي                                                                                           |
| --------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `DEFINER alice` | يجب أن يمتلك `alice` صلاحية `SELECT` على الجدول المصدر للعرض.  | يجب أن يمتلك `alice` صلاحية `SELECT` على الجدول المصدر للعرض، وصلاحية `INSERT` على الجدول الهدف للعرض. |
| `INVOKER`       | يجب أن يمتلك المستخدم صلاحية `SELECT` على الجدول المصدر للعرض. | لا يمكن تحديد `SQL SECURITY INVOKER` للعروض المادية.                                                   |
| `NONE`          | -                                                              | -                                                                                                      |

:::note
يُعد `SQL SECURITY NONE` خيارًا مهجورًا. سيتمكن أي مستخدم لديه صلاحية إنشاء عروض باستخدام `SQL SECURITY NONE` من تنفيذ أي استعلام اعتباطي.
لذلك، يلزم وجود `GRANT ALLOW SQL SECURITY NONE TO <user>` لإنشاء عرض باستخدام هذا الخيار.
:::

إذا لم يتم تحديد `DEFINER`/`SQL SECURITY`، فستُستخدم القيم الافتراضية:

* `SQL SECURITY`: ‏`INVOKER` للعروض العادية و`DEFINER` للعروض المادية ([قابل للتهيئة عبر الإعدادات](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER`: ‏`CURRENT_USER` ([قابل للتهيئة عبر الإعدادات](../../../operations/settings/settings.md#default_view_definer))

إذا أُرفق عرض من دون تحديد `DEFINER`/`SQL SECURITY`، فستكون القيمة الافتراضية هي `SQL SECURITY NONE` للعرض المادي و`SQL SECURITY INVOKER` للعرض العادي.

لتغيير أمان SQL لعرض موجود، استخدم

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### أمثلة
</div>

```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

هذه الميزة متقادمة وستُزال مستقبلًا.

لتسهيل الأمر عليك، يمكنك العثور على الوثائق القديمة [هنا](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

<div id="refreshable-materialized-view">
  ## العرض المادي القابل للتحديث
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

حيث يكون `interval` سلسلة من الفترات الزمنية البسيطة:

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

يجب أن يحدِّد بند `REFRESH` واحدًا على الأقل من `EVERY` أو `AFTER` أو `DEPENDS ON`. ويُرفض استخدام `REFRESH` بمفرده (من دون أيٍّ من هذه الخيارات). كما أن `REFRESH DEPENDS ON ...` من دون `EVERY`/`AFTER` هو صيغة مختصرة لـ `REFRESH AFTER 0 SECOND DEPENDS ON ...`؛ راجع [تبعيات التحديث](#refresh-dependencies) أدناه.

يُشغِّل الاستعلام المقابل دوريًا ويخزّن نتيجته في جدول.

* إذا تم تحديد `APPEND`، فإن كل عملية تحديث تُدرج صفوفًا في الجدول من دون حذف الصفوف الموجودة. وعملية الإدراج ليست ذرّية، تمامًا مثل استعلام `INSERT INTO ... SELECT` العادي.
* بخلاف ذلك، يستبدل كل تحديث محتويات الجدول السابقة بشكل ذرّي.

الاختلافات عن العروض المادية العادية غير القابلة للتحديث:

* لا يوجد insert trigger. عند إدراج بيانات جديدة في الجدول المحدد في `SELECT`، فإنها *لا* تُرسَل تلقائيًا إلى العرض المادي القابل للتحديث. بدلًا من ذلك، لا يتم إدراج البيانات إلا أثناء تشغيلات التحديث الدورية أو اليدوية.
* لا توجد قيود على استعلام `SELECT`. فجميع دوال الجداول (مثل `url()`)، والعروض، وUNION، وJOIN، مسموح بها.

:::note
الإعدادات الموجودة في جزء `REFRESH ... SETTINGS` من الاستعلام هي إعدادات التحديث (مثل `refresh_retries`)، وهي مختلفة عن الإعدادات العادية (مثل `max_threads`). ويمكن تحديد الإعدادات العادية باستخدام `SETTINGS` في نهاية الاستعلام.
:::

<div id="refresh-schedule">
  ### جدول التحديث
</div>

أمثلة على جداول التحديث:

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

يُعدّل `RANDOMIZE FOR` وقت كل عملية تحديث بشكل عشوائي، على سبيل المثال:

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

لا يمكن أن تكون هناك أكثر من عملية تحديث واحدة قيد التشغيل في الوقت نفسه لعرض معيّن. على سبيل المثال، إذا كان عرض يحتوي على `REFRESH EVERY 1 MINUTE` يستغرق دقيقتين لإجراء تحديث، فسيُجرى تحديث كل دقيقتين فقط. وإذا أصبح بعد ذلك أسرع وبدأ يُجرى خلال 10 ثوانٍ، فسيعود إلى إجراء تحديث كل دقيقة. (وعلى وجه الخصوص، لن يُجرى تحديث كل 10 ثوانٍ لتعويض تحديثات فائتة متراكمة — إذ لا يوجد مثل هذا التراكم.)

عادةً ما يبدأ أول تحديث مباشرةً بعد إنشاء materialized view: فالوقت المنقضي منذ آخر تحديث يساوي ما لا نهاية، لذا يشير أي schedule إلى أن وقت إجراء تحديث قد حان الآن. وإذا تم تحديد `EMPTY`، فسيتم تخطي هذا التحديث الأولي، ويحدث أول تحديث في الوقت المجدول التالي؛ فعلى سبيل المثال، مع `EVERY 1 HOUR` سيحدث أول تحديث عند نهاية الساعة الحالية.

<div id="in-replicated-db">
  ### في DB ‏Replicated
</div>

إذا كان العرض المادي القابل للتحديث موجودًا في [قاعدة بيانات Replicated](../../../engines/database-engines/replicated.md)، فإن النسخ المتماثلة تنسّق فيما بينها بحيث لا تنفّذ التحديث في كل وقت مجدول إلا نسخة متماثلة واحدة. ويُشترط استخدام محرك الجداول [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md)، حتى تتمكن جميع النسخ المتماثلة من رؤية البيانات الناتجة عن التحديث.

في وضع `APPEND`، يمكن تعطيل التنسيق باستخدام `SETTINGS all_replicas = 1`. وهذا يجعل النسخ المتماثلة تنفّذ التحديثات بشكل مستقل عن بعضها. وفي هذه الحالة، لا يكون ReplicatedMergeTree مطلوبًا.

في الوضع غير `APPEND`، لا يُدعَم إلا التحديث المنسَّق. أما إذا أردت تحديثًا غير منسَّق، فاستخدم قاعدة بيانات `Atomic` واستعلام `CREATE ... ON CLUSTER` لإنشاء عروض مادية قابلة للتحديث على جميع النسخ المتماثلة.

يتم التنسيق عبر Keeper. ويُحدَّد مسار znode بواسطة إعداد الخادم [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path).

<div id="refresh-dependencies">
  ### تبعيات التحديث
</div>

تُزامِن `DEPENDS ON` عمليات تحديث الجداول المختلفة:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

لن يبدأ تحديث العرض التابع إلا بعد اكتمال تحديث جميع العروض التي يعتمد عليها.

للتحديث فورًا بعد تحديث عرض آخر:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

أو بصورة مكافئة:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` لا يعمل إلا بين العروض المادية القابلة للتحديث. وعلى وجه الخصوص، إذا كان عرض التبعية يستخدم `TO <table>`، فتأكد من استخدام اسم العرض بدلًا من اسم الجدول. وإذا كانت قائمة `DEPENDS ON` تتضمن جدولًا عاديًا أو عرضًا غير قابل للتحديث أو تحتوي على خطأ مطبعي، فلن يتم تحديث العرض مطلقًا، وستظهر حالته على أنها `MissingDependencies` في `system.view_refreshes`. يمكن تغيير التبعيات أو إزالتها باستخدام `ALTER`، راجع [تغيير معلمات التحديث](#changing-refresh-parameters).
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### استخدام `DEPENDS ON` لضمان زمن انتشار متسق
</div>

إذا كان كلا العرضين يستخدم `REFRESH EVERY` بالفترة نفسها، فستُطبَّق التبعية في كل فترة زمنية.

على سبيل المثال، افترض أن العرضين X وY يستخدمان كلاهما `REFRESH EVERY 1 HOUR`، وأن Y يقرأ من جدول المخرجات الخاص بـ X. من دون تبعيات، سيرى Y عادةً بيانات X من تحديث الساعة السابقة. ومع `DEPENDS ON X`، لن يبدأ تحديث Y عند 11:00 إلا بعد اكتمال تحديث X عند 11:00.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

قد يتخطّى كلٌّ من العنصر الذي يعتمد عليه والعنصر التابع فتراتٍ زمنيةً بشكل مستقل إذا استغرقت عمليات التحديث وقتًا أطول من فترة التحديث. ولا يوجد ما يضمن أن يُحدَّث العنصر التابع مرةً واحدةً بالضبط مقابل كل تحديث للعنصر الذي يعتمد عليه.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### استخدام DEPENDS ON لمعالجة الدفق على دفعات
</div>

إذا لم يُستخدم `REFRESH EVERY`، فسيُحدَّث العرض التابع X إذا كانت جميع تبعياته قد حُدِّثت مرة واحدة على الأقل منذ آخر تحديث لـ X. ويضيف `REFRESH AFTER T` تأخيرًا: سيبدأ التابع التحديث بعد مدة T من اكتمال تحديث التبعية.

التبعيات الدائرية مسموح بها ومفيدة. تأمل هذا المخطط للعروض المادية القابلة للتحديث:

1. يأخذ X دفعة من الصفوف من دفقٍ ما ويضعها في جدول.
2. ثم يقرأ كل من Y و Z من ذلك الجدول، ويجريان عمليات تجميع مختلفة، ويُلحقان النتائج بجداول أخرى.
3. بعد اكتمال معالجة الدفعة بالكامل، يأخذ X الدفعة التالية، وتتكرر الدورة.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

مثال كامل:

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

تعمل السلاسل الأطول أيضًا.

لا ينجح هذا بشكل جيد إلا عند تمكين تنسيق التحديث، أي عندما تكون العروض في قاعدة بيانات Replicated أو Shared. ومن دون هذا التنسيق، تؤدي إعادة تشغيل الخادم إلى قطع التسلسل، مما يستلزم تشغيل `SYSTEM REFRESH VIEW` يدويًا بعد كل إعادة تشغيل بدلًا من تشغيله مرة واحدة فقط بعد إنشاء العروض.

<div id="refresh-settings">
  ### إعدادات التحديث
</div>

إعدادات التحديث المتاحة:

* `refresh_retries` - عدد مرات إعادة المحاولة إذا فشل استعلام التحديث بسبب استثناء. إذا فشلت جميع محاولات إعادة المحاولة، فانتقل إلى وقت التحديث المجدول التالي. تعني 0 عدم إجراء أي إعادة محاولة، وتعني -1 إعادة المحاولة بلا حدود. القيمة الافتراضية: 2.
* `refresh_retry_initial_backoff_ms` - مدة التأخير قبل أول إعادة محاولة، إذا لم تكن `refresh_retries` تساوي صفرًا. تتضاعف مدة التأخير مع كل إعادة محاولة لاحقة، حتى `refresh_retry_max_backoff_ms`. القيمة الافتراضية: 100 مللي ثانية.
* `refresh_retry_max_backoff_ms` - الحد الأقصى للنمو الأُسّي لمدة التأخير بين محاولات التحديث. القيمة الافتراضية: 60000 مللي ثانية (دقيقة واحدة).
* `all_replicas` - في [قاعدة بيانات Replicated](../../../engines/database-engines/replicated.md) مع `APPEND`، يحدد ما إذا كانت جميع النسخ المتماثلة تُحدِّث بشكل مستقل، أو ما إذا كانت نسخة متماثلة واحدة فقط تُجري التحديث عند كل وقت مجدول. لا يمكن تغيير هذا بعد إنشاء العرض. القيمة الافتراضية: `false`.

<div id="changing-refresh-parameters">
  ### تغيير معلمات التحديث
</div>

تُغيَّر معلمات التحديث لعرض مادي قابل للتحديث موجود باستخدام [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

الجدولة (`EVERY` أو `AFTER`) إلزامية: إذ تستبدل التعليمة دائمًا *جميع* معلمات التحديث — الجدولة، و`RANDOMIZE FOR`، و`DEPENDS ON`، وإعدادات التحديث — بما هو محدد. وأي عنصر يتم إغفاله يُعاد ضبطه على قيمته الافتراضية (بالنسبة إلى الإعدادات) أو يُزال (بالنسبة إلى التبعيات والتوزيع العشوائي).

:::note

* لتغيير إعدادات التحديث فقط (مثل `refresh_retries`)، أعد ذكر الجدولة الحالية:

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* ‎`ALTER TABLE ... MODIFY SETTING refresh_retries = ...` غير مدعوم للعروض المادية؛ ويجب استخدام `MODIFY REFRESH`.

* إضافة `APPEND` أو إزالته غير مدعوم.

* لا يمكن تغيير الإعداد `all_replicas` بعد الإنشاء.
  :::

أمثلة:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### عمليات أخرى
</div>

تتوفر حالة جميع العروض المادية القابلة للتحديث في الجدول [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). وعلى وجه الخصوص، يتضمن تقدّم التحديث (إذا كان قيد التنفيذ)، ووقت آخر تحديث ووقت التحديث التالي، ورسالة الاستثناء إذا فشل التحديث.

لإيقاف التحديثات أو تشغيلها أو تشغيلها يدويًا أو إلغائها، استخدم [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views).

للانتظار حتى يكتمل التحديث، استخدم [`SYSTEM WAIT VIEW`](../system.md#wait-view). ويكون ذلك مفيدًا بشكل خاص عند انتظار التحديث الأولي بعد إنشاء عرض.

:::note
معلومة طريفة: يُسمح لاستعلام التحديث بالقراءة من العرض الجاري تحديثه، مع رؤية إصدار البيانات السابق للتحديث. وهذا يعني أنه يمكنك تنفيذ لعبة الحياة لـ Conway: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## Window View
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
هذه ميزة تجريبية، وقد تتغير مستقبلًا على نحوٍ غير متوافق مع الإصدارات السابقة. فعِّل استخدام Window View واستعلام `WATCH` باستخدام الإعداد [allow&#95;experimental&#95;window&#95;view](/ar/operations/settings/settings#allow_experimental_window_view). أدخِل الأمر `set allow_experimental_window_view = 1`.
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

يمكن لـ window view تجميع البيانات حسب النافذة الزمنية وإخراج النتائج عندما تصبح النافذة جاهزة للإطلاق. وهي تخزّن نتائج التجميع الجزئية في table داخلية (أو محددة) لتقليل latency، ويمكنها دفع نتيجة المعالجة إلى table محددة أو إرسال إشعارات دفع باستخدام استعلام `WATCH`.

يشبه إنشاء window view إنشاء `MATERIALIZED VIEW`. تحتاج window view إلى محرك تخزين داخلي لتخزين البيانات الوسيطة. ويمكن تحديد التخزين الداخلي باستخدام عبارة `INNER ENGINE`، وستستخدم window view المحرك `AggregatingMergeTree` بوصفه محركها الداخلي الافتراضي.

عند إنشاء window view من دون `TO [db].[table]`، يجب عليك تحديد `ENGINE` — وهو محرك الجدول المستخدم لتخزين البيانات.

<div id="time-window-functions">
  ### دوال النافذة الزمنية
</div>

تُستخدم [دوال النافذة الزمنية](../../functions/time-window-functions.md) للحصول على الحدّين الأدنى والأعلى لنافذة السجلات. ويجب استخدام window view مع دالة نافذة زمنية.

<div id="time-attributes">
  ### سمات الوقت
</div>

يدعم Window View كُلًا من **وقت المعالجة** و**وقت الحدث**.

يتيح **وقت المعالجة** لـ Window View إنشاء النتائج استنادًا إلى وقت الجهاز المحلي، ويُستخدم افتراضيًا. وهو أبسط مفهوم للوقت، لكنه لا يوفّر الحتمية. ويمكن تعريف سمة وقت المعالجة من خلال تعيين `time_attr` في دالة النافذة الزمنية إلى عمود في جدول أو باستخدام الدالة `now()`. ينشئ الاستعلام التالي Window View باستخدام وقت المعالجة.

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**وقت الحدث** هو الوقت الذي وقع فيه كل حدث على الجهاز الذي أنشأه. وعادةً ما يكون هذا الوقت مضمّنًا داخل السجلات عند إنشائها. تتيح معالجة وقت الحدث الحصول على نتائج متسقة حتى في حالة الأحداث الخارجة عن الترتيب أو المتأخرة. ويدعم Window View معالجة وقت الحدث باستخدام صيغة `WATERMARK`.

يوفر Window View ثلاث استراتيجيات للعلامة المائية:

* `STRICTLY_ASCENDING`: يُصدر علامة مائية تمثل أكبر طابع زمني تمت ملاحظته حتى الآن. ولا تُعدّ الصفوف التي لها طابع زمني أصغر من أكبر طابع زمني متأخرة.
* `ASCENDING`: يُصدر علامة مائية تمثل أكبر طابع زمني تمت ملاحظته حتى الآن مطروحًا منه 1. ولا تُعدّ الصفوف التي لها طابع زمني يساوي أكبر طابع زمني أو يقل عنه متأخرة.
* `BOUNDED`: WATERMARK=INTERVAL. يُصدر علامات مائية تمثل أكبر طابع زمني تمت ملاحظته مطروحًا منه مقدار التأخير المحدد.

الاستعلامات التالية أمثلة على إنشاء Window View باستخدام `WATERMARK`:

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

بشكلٍ افتراضي، تُطلِق النافذة نتيجتها عند وصول العلامة المائية، وتُسقَط العناصر التي تصل بعد العلامة المائية. تدعم Window View معالجة الأحداث المتأخرة من خلال تعيين `ALLOWED_LATENESS=INTERVAL`. ومثال على التعامل مع التأخر هو:

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

لاحظ أن العناصر المنبعثة من إطلاق متأخر يجب التعامل معها بوصفها نتائج محدَّثة لعملية حساب سابقة. وبدلًا من الإطلاق عند نهاية النوافذ، فإن Window View سيُطلِق فور وصول الحدث المتأخر. لذلك، سينتج عن ذلك مخرجات متعددة للنافذة نفسها. ويجب على المستخدمين مراعاة هذه النتائج المكررة أو إزالة تكرارها.

يمكنك تعديل استعلام `SELECT` المحدد في Window View باستخدام العبارة `ALTER TABLE ... MODIFY QUERY`. يجب أن تكون بنية البيانات الناتجة عن استعلام `SELECT` الجديد مطابقة لبنية استعلام `SELECT` الأصلي، سواء وُجد البند `TO [db.]name` أم لم يوجد. لاحظ أن البيانات في النافذة الحالية ستُفقد، لأن الحالة الوسيطة لا يمكن إعادة استخدامها.

<div id="monitoring-new-windows">
  ### مراقبة النوافذ الجديدة
</div>

يدعم Window view استعلام [WATCH](../../../sql-reference/statements/watch.md) لمراقبة التغييرات، أو يمكن استخدام صيغة `TO` لإرسال النتائج إلى جدول.

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

يمكن تحديد `LIMIT` لتعيين عدد التحديثات التي سيتم تلقيها قبل إنهاء الاستعلام. ويمكن استخدام عبارة `EVENTS` للحصول على صيغة مختصرة من استعلام `WATCH`، بحيث تحصل، بدلًا من نتيجة الاستعلام، على أحدث علامة مائية له فقط.

<div id="settings-1">
  ### الإعدادات
</div>

* `window_view_clean_interval`: الفاصل الزمني لتنظيف `window view` بالثواني لتحرير البيانات القديمة. سيحتفظ النظام بالنوافذ التي لم تُفعَّل بالكامل وفقًا لوقت النظام أو لإعداد `WATERMARK`، وستُحذف البيانات الأخرى.
* `window_view_heartbeat_interval`: الفاصل الزمني لنبضات الحياة بالثواني للدلالة على أن استعلام watch لا يزال نشطًا.
* `wait_for_window_view_fire_signal_timeout`: مهلة انتظار إشارة تشغيل `window view` في معالجة وقت الحدث.

<div id="example">
  ### مثال
</div>

لنفترض أننا نحتاج إلى حساب عدد سجلات النقرات لكل 10 ثوانٍ في جدول سجلات يُسمّى `data`، ويكون هيكل الجدول كما يلي:

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

أولاً، ننشئ window view بنافذة tumble بفاصل زمني مدته 10 ثوانٍ:

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

بعد ذلك، نستخدم الاستعلام `WATCH` للحصول على النتائج.

```sql
WATCH wv
```

عند إدراج السجلات في الجدول `data`،

```sql
INSERT INTO data VALUES(1,now())
```

يجب أن يعرض الاستعلام `WATCH` النتائج على النحو التالي:

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

بدلًا من ذلك، يمكننا توجيه المخرجات إلى جدول آخر باستخدام صيغة `TO`.

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

يمكن العثور على أمثلة إضافية ضمن اختبارات ClickHouse ذات الحالة (وتحمل هناك الاسم `*window_view*`).

<div id="window-view-usage">
  ### استخدام window view
</div>

تكون `window view` مفيدة في السيناريوهات التالية:

* **المراقبة**: تجميع سجلات المقاييس وحسابها حسب الوقت، ثم إخراج النتائج إلى جدول هدف. ويمكن أن تستخدم لوحة المعلومات جدول الهدف بوصفه جدولًا مصدرًا.
* **التحليل**: تجميع البيانات ومعالجتها مسبقًا تلقائيًا ضمن النافذة الزمنية. ويكون ذلك مفيدًا عند تحليل عدد كبير من السجلات. وتُلغي المعالجة المسبقة الحسابات المتكررة عبر استعلامات متعددة وتقلل زمن استجابة الاستعلام.

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [العمل مع بيانات السلاسل الزمنية في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* مدونة: [بناء حل Observability باستخدام ClickHouse - الجزء 2 - التتبعات](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## العروض المؤقتة
</div>

يدعم ClickHouse **العروض المؤقتة** بالخصائص التالية (على غرار الجداول المؤقتة حيثما ينطبق ذلك):

* **مدة الجلسة**
  لا يوجد العرض المؤقت إلا طوال مدة الجلسة الحالية. ويُحذف تلقائيًا عند انتهاء الجلسة.

* **بدون قاعدة بيانات**
  **لا يمكنك** إسناد العرض المؤقت إلى اسم قاعدة بيانات. فهو يوجد خارج قواعد البيانات (ضمن نطاق الجلسة).

* **غير مكررة / بدون ON CLUSTER**
  الكائنات المؤقتة محلية ضمن الجلسة، و**لا يمكن** إنشاؤها باستخدام `ON CLUSTER`.

* **حلّ الأسماء**
  إذا كان لكائن مؤقت (جدول أو عرض) الاسم نفسه لكائن دائم، وأشار استعلام إلى الاسم **من دون** قاعدة بيانات، فسيُستخدم الكائن **المؤقت**.

* **كائن منطقي (من دون تخزين)**
  لا يخزن العرض المؤقت سوى نص `SELECT` الخاص به فقط (ويستخدم `View` داخليًا للتخزين). ولا يحتفظ بالبيانات ولا يقبل `INSERT`.

* **بند ENGINE**
  **لا** تحتاج إلى تحديد `ENGINE`؛ وإذا تم تقديمه على هيئة `ENGINE = View`، فسيُتجاهل/يُعامل على أنه العرض المنطقي نفسه.

* **الأمان / الامتيازات**
  يتطلب إنشاء عرض مؤقت الامتياز `CREATE TEMPORARY VIEW`، والذي يُمنح ضمنيًا بواسطة `CREATE VIEW`.

* **SHOW CREATE**
  استخدم `SHOW CREATE TEMPORARY VIEW view_name;` لطباعة DDL لعرض مؤقت.

<div id="temporary-views-syntax">
  ### الصيغة
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` **غير** مدعوم مع العروض المؤقتة (تماشيًا مع الجداول المؤقتة). إذا احتجت إلى «استبدال» عرض مؤقت، فاحذفها ثم أنشئها من جديد.

<div id="examples">
  ### أمثلة
</div>

أنشئ جدول مصدر مؤقتًا وعرضًا مؤقتًا يعتمد عليه:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

اعرض عبارة DDL الخاصة به:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

احذفه:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### غير المسموح به / القيود
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **غير مسموح به** (استخدم `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **غير مسموح به**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **غير مسموح به** (من دون محدِّد database).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **غير مسموح به** (الكائنات المؤقتة محلية ضمن session).
* `POPULATE`, `REFRESH`, `TO [db.table]`, المحركات الداخلية، وجميع clauses الخاصة بـ MV → **لا تنطبق** على عرض مؤقت.

<div id="temporary-views-distributed-notes">
  ### ملاحظات حول الاستعلامات الموزعة
</div>

**العرض** المؤقت ليس سوى تعريف؛ إذ لا توجد بيانات ليجري تمريرها. وإذا كان العرض المؤقت يشير إلى **جداول** مؤقتة (مثل `Memory`)، فيمكن نقل بياناتها إلى الخوادم البعيدة أثناء تنفيذ الاستعلامات الموزعة، تمامًا كما هو الحال مع الجداول المؤقتة.

<div id="temporary-views-distributed-example">
  #### مثال
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```