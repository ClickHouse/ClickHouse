---
description: 'توثيق PARTITION'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'معالجة الأقسام والأجزاء'
doc_type: 'reference'
---

العمليات التالية على [الأقسام](/ar/engines/table-engines/mergetree-family/custom-partitioning-key.md) متاحة:

* [DETACH PARTITION|PART](#detach-partitionpart) — ينقل قسمًا أو جزءًا إلى الدليل `detached` ثم يفصله عنه.
* [DROP PARTITION|PART](#drop-partitionpart) — يحذف قسمًا أو جزءًا.
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - يحذف جزءًا أو جميع أجزاء قسم من `detached`.
* [FORGET PARTITION](#forget-partition) — يحذف البيانات الوصفية للقسم من ZooKeeper إذا كان فارغًا.
* [ATTACH PARTITION|PART](#attach-partitionpart) — يضيف قسمًا أو جزءًا من الدليل `detached` إلى الجدول.
* [ATTACH PARTITION FROM](#attach-partition-from) — ينسخ قسم البيانات من جدول إلى آخر ثم يضيفه.
* [REPLACE PARTITION](#replace-partition) — ينسخ قسم البيانات من جدول إلى آخر ثم يستبدله.
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — ينقل قسم البيانات من جدول إلى آخر.
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — يعيد تعيين قيمة عمود محدد في قسم.
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — يعيد تعيين الفهرس الثانوي المحدد في قسم.
* [FREEZE PARTITION](#freeze-partition) — ينشئ نسخة احتياطية لقسم.
* [UNFREEZE PARTITION](#unfreeze-partition) — يزيل نسخة احتياطية لقسم.
* [FETCH PARTITION|PART](#fetch-partitionpart) — ينزّل جزءًا أو قسمًا من خادم آخر.
* [MOVE PARTITION|PART](#move-partitionpart) — ينقل قسمًا/جزء بيانات إلى قرص أو وحدة تخزين أخرى.
* [UPDATE IN PARTITION](#update-in-partition) — يحدّث البيانات داخل القسم وفقًا لشرط.
* [DELETE IN PARTITION](#delete-in-partition) — يحذف البيانات داخل القسم وفقًا لشرط.
* [REWRITE PARTS](#rewrite-parts) — يعيد كتابة الأجزاء في الجدول (أو في قسم محدد) بالكامل.

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

ينقل جميع البيانات الخاصة بالقسم المحدد إلى دليل `detached`. ويتجاهل الخادم قسم البيانات المفصول هذا كما لو أنه غير موجود. ولن يتعرّف الخادم على هذه البيانات حتى تُجري استعلام [ATTACH](#attach-partitionpart).

مثال:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

اقرأ عن تعيين تعبير التقسيم في قسم [كيفية تعيين تعبير التقسيم](#how-to-set-partition-expression).

بعد تنفيذ الاستعلام، يمكنك فعل ما تشاء بالبيانات الموجودة في الدليل `detached` — حذفها من نظام الملفات أو تركها كما هي.

هذا الاستعلام مكرّر — إذ ينقل البيانات إلى الدليل `detached` على جميع النسخ المتماثلة. لاحظ أنه لا يمكنك تنفيذ هذا الاستعلام إلا على نسخة متماثلة قائدة. ولمعرفة ما إذا كانت نسخة متماثلة ما قائدة، نفّذ استعلام `SELECT` على جدول [system.replicas](/ar/operations/system-tables/replicas). وبدلًا من ذلك، من الأسهل تنفيذ استعلام `DETACH` على جميع النسخ المتماثلة — إذ إن جميع النسخ المتماثلة تُثير استثناءً، باستثناء النسخ المتماثلة القائدة (لأنه يُسمح بوجود عدة قادة).

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

يحذف القسم المحدد من الجدول. يضع هذا الاستعلام علامة على القسم على أنه غير نشط، ويحذف البيانات بالكامل خلال نحو 10 دقائق.

اطلع على كيفية تعيين تعبير القسم في قسم [كيفية تعيين تعبير القسم](#how-to-set-partition-expression).

الاستعلام مُكرَّر، لذا يحذف البيانات على جميع النسخ المتماثلة.

مثال:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

يزيل الجزء المحدد أو جميع الأجزاء الخاصة بالقسم المحدد من `detached`.
اقرأ المزيد عن كيفية تعيين تعبير القسم في قسم [كيفية تعيين تعبير القسم](#how-to-set-partition-expression).

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

يزيل جميع البيانات الوصفية الخاصة بقسم فارغ من ZooKeeper. يفشل الاستعلام إذا لم يكن القسم فارغًا أو كان غير معروف. احرص على تنفيذ ذلك فقط على الأقسام التي لن تُستخدم مرة أخرى مطلقًا.

اقرأ عن تعيين تعبير القسم في قسم [كيفية تعيين تعبير القسم](#how-to-set-partition-expression).

مثال:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

يضيف البيانات إلى الجدول من الدليل `detached`. يمكن إضافة بيانات لقسم كامل أو لجزء منفصل. أمثلة:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

اقرأ المزيد حول تعيين تعبير التقسيم في قسم [كيفية تعيين تعبير التقسيم](#how-to-set-partition-expression).

هذا الاستعلام مُكرَّر. تتحقق النسخة المتماثلة المُبادِرة مما إذا كانت هناك بيانات في الدليل `detached`.
إذا كانت البيانات موجودة، يتحقق الاستعلام من سلامتها. وإذا كان كل شيء صحيحًا، يضيف الاستعلام البيانات إلى الجدول.

إذا عثرت النسخة المتماثلة غير المُبادِرة، التي تتلقى أمر ATTACH، على الجزء ذي قيم التحقق الصحيحة في المجلد `detached` الخاص بها، فإنها تُرفق البيانات من دون جلبها من نُسخ متماثلة أخرى.
إذا لم يكن هناك جزء ذو قيم التحقق الصحيحة، فتُنزَّل البيانات من أي نسخة متماثلة تحتوي على هذا الجزء.

يمكنك وضع البيانات في الدليل `detached` على إحدى النُّسخ المتماثلة واستخدام استعلام `ALTER ... ATTACH` لإضافتها إلى الجدول على جميع النُّسخ المتماثلة.

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

ينسخ هذا الاستعلام قسم البيانات من `table1` إلى `table2`.

لاحظ ما يلي:

* لن تُحذف أي بيانات من `table1` أو `table2`.
* يمكن أن يكون `table1` جدولًا مؤقتًا.

لكي يُنفَّذ الاستعلام بنجاح، يجب استيفاء الشروط التالية:

* يجب أن يكون للجدولين البنية نفسها.
* يجب أن يكون للجدولين مفتاح التقسيم نفسه، ومفتاح ORDER BY نفسه، والمفتاح الأساسي نفسه.
* يجب أن تكون للجدولين سياسة التخزين نفسها.
* يجب أن يتضمن جدول الوجهة جميع الفهارس والإسقاطات الموجودة في جدول المصدر. وإذا كان الإعداد `enforce_index_structure_match_on_partition_manipulation` مُمكّنًا في جدول الوجهة، فيجب أن تكون الفهارس والإسقاطات متطابقة. وإلا، فيمكن أن يضم جدول الوجهة مجموعة فائقة من فهارس جدول المصدر وإسقاطاته.

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

ينسخ هذا الاستعلام قسم البيانات من `table1` إلى `table2` ويستبدل القسم الموجود في `table2`. هذه العملية ذرية.

لاحظ ما يلي:

* لن تُحذف البيانات من `table1`.
* قد يكون `table1` جدولًا مؤقتًا.

لكي يُنفَّذ الاستعلام بنجاح، يجب استيفاء الشروط التالية:

* يجب أن يكون للجدولين البنية نفسها.
* يجب أن يكون للجدولين مفتاح التقسيم نفسه، ومفتاح ORDER BY نفسه، والمفتاح الأساسي نفسه.
* يجب أن تكون للجدولين سياسة التخزين نفسها.
* يجب أن يتضمن جدول الوجهة جميع الفهارس والإسقاطات الموجودة في جدول المصدر. إذا كان الإعداد `enforce_index_structure_match_on_partition_manipulation` مُمكّنًا في جدول الوجهة، فيجب أن تكون الفهارس والإسقاطات متطابقة تمامًا. وإلا، فيمكن أن يتضمن جدول الوجهة مجموعةً أوسع من فهارس جدول المصدر وإسقاطاته.

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

ينقل هذا الاستعلام قسم البيانات من `table_source` إلى `table_dest` مع حذف البيانات من `table_source`.

لكي يُنفَّذ الاستعلام بنجاح، يجب استيفاء الشروط التالية:

* يجب أن يكون للجدولين البنية نفسها.
* يجب أن يكون للجدولين مفتاح التقسيم نفسه، ومفتاح ORDER BY نفسه، والمفتاح الأساسي نفسه.
* يجب أن تكون للجدولين سياسة التخزين نفسها.
* يجب أن ينتمي الجدولان إلى الفئة نفسها من المحرّكات (مكرّر أو غير مكرّر).
* يجب أن يتضمّن الجدول الوجهة جميع الفهارس والإسقاطات الموجودة في الجدول المصدر. إذا كان الإعداد `enforce_index_structure_match_on_partition_manipulation` مفعّلًا في الجدول الوجهة، فيجب أن تكون الفهارس والإسقاطات متطابقة. وإلا، يمكن أن يحتوي الجدول الوجهة على مجموعة أشمل من فهارس الجدول المصدر وإسقاطاته.

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

يعيد ضبط جميع القيم في العمود المحدد داخل قسم. وإذا كانت عبارة `DEFAULT` قد حُدِّدت عند إنشاء جدول، فإن هذا الاستعلام يعيّن قيمة العمود إلى القيمة الافتراضية المحددة.

مثال:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

ينشئ هذا الاستعلام نسخة احتياطية محلية لقسم محدد. وإذا أُهمل الشرط `PARTITION`، فإن الاستعلام ينشئ نسخة احتياطية لجميع الأقسام دفعة واحدة.

:::note
تُنفَّذ عملية النسخ الاحتياطي بالكامل من دون إيقاف الخادم.
:::

لاحظ أنه بالنسبة للجداول القديمة النمط، يمكنك تحديد بادئة اسم القسم (على سبيل المثال، `2019`) — وعندئذٍ ينشئ الاستعلام نسخة احتياطية لجميع الأقسام المطابقة. اقرأ عن تعيين تعبير التقسيم في قسم [كيفية تعيين تعبير التقسيم](#how-to-set-partition-expression).

عند التنفيذ، ولإنشاء لقطة بيانات، ينشئ الاستعلام روابط صلبة لبيانات الجدول. وتوضَع هذه الروابط الصلبة في الدليل `/var/lib/clickhouse/shadow/N/...`، حيث:

* يمثّل `/var/lib/clickhouse/` دليل العمل الخاص بـ ClickHouse والمحدَّد في config.
* يمثّل `N` الرقم التزايدي للنسخة الاحتياطية.
* إذا جرى تحديد المعامل `WITH NAME`، فستُستخدم قيمة المعامل `'backup_name'` بدلًا من الرقم التزايدي.

:::note
إذا كنت تستخدم [مجموعة من الأقراص لتخزين البيانات في جدول](/ar/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes)، فسيظهر الدليل `shadow/N` على كل قرص، مع تخزين أجزاء البيانات التي طابقها تعبير `PARTITION`.
:::

يُنشأ داخل النسخة الاحتياطية نفس هيكل الأدلة الموجود داخل `/var/lib/clickhouse/`. وينفّذ الاستعلام `chmod` على جميع الملفات، مانعًا الكتابة إليها.

بعد إنشاء النسخة الاحتياطية، يمكنك نسخ البيانات من `/var/lib/clickhouse/shadow/` إلى الخادم البعيد ثم حذفها من الخادم المحلي. لاحظ أن الاستعلام `ALTER t FREEZE PARTITION` غير مُكرَّر. فهو ينشئ نسخة احتياطية محلية على الخادم المحلي فقط.

ينشئ الاستعلام النسخة الاحتياطية على نحو شبه فوري (لكنه ينتظر أولًا حتى تنتهي الاستعلامات الحالية على الجدول المعني من التنفيذ).

ينسخ `ALTER TABLE t FREEZE PARTITION` البيانات فقط، وليس البيانات الوصفية للجدول. ولإنشاء نسخة احتياطية من البيانات الوصفية للجدول، انسخ الملف `/var/lib/clickhouse/metadata/database/table.sql`

لاستعادة البيانات من نسخة احتياطية، اتبع ما يلي:

1. أنشئ الجدول إذا لم يكن موجودًا. ولعرض الاستعلام، استخدم ملف ‎`.sql`‎ (واستبدل `ATTACH` فيه بـ `CREATE`).
2. انسخ البيانات من الدليل `data/database/table/` داخل النسخة الاحتياطية إلى الدليل `/var/lib/clickhouse/data/database/table/detached/`.
3. شغّل استعلامات `ALTER TABLE t ATTACH PARTITION` لإضافة البيانات إلى الجدول.

لا تتطلب الاستعادة من نسخة احتياطية إيقاف الخادم.

يعالج الاستعلام الأجزاء بالتوازي، ويُنظَّم عدد سلاسل التنفيذ بواسطة الإعداد `max_threads`.

لمزيد من المعلومات حول النسخ الاحتياطية واستعادة البيانات، راجع قسم [&quot;النسخ الاحتياطي والاستعادة في ClickHouse&quot;](/ar/operations/backup/overview).

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

يزيل الأقسام `frozen` التي تحمل الاسم المحدد من القرص. إذا أُهمل بند `PARTITION`، فسيزيل الاستعلام النسخة الاحتياطية لجميع الأقسام دفعة واحدة.

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

يعمل هذا الاستعلام بطريقة مشابهة لـ `CLEAR COLUMN`، لكنه يعيد تعيين فهرس بدلًا من بيانات العمود.

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

ينزّل قسمًا من خادم آخر. لا يعمل هذا الاستعلام إلا مع الجداول المكرّرة.

ينفّذ الاستعلام ما يلي:

1. ينزّل القسم|الجزء من الـ shard المحدد. في &#39;path-in-zookeeper&#39; يجب تحديد مسار إلى الـ shard في ZooKeeper.
2. ثم يضع الاستعلام البيانات التي تم تنزيلها في الدليل `detached` للجدول `table_name`. استخدم الاستعلام [ATTACH PARTITION|PART](#attach-partitionpart) لإضافة البيانات إلى الجدول.

على سبيل المثال:

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

لاحظ ما يلي:

* إن استعلام `ALTER ... FETCH PARTITION|PART` لا يُنسخ إلى النسخ المتماثلة. فهو يضع الجزء أو القسم في الدليل `detached` على الخادم المحلي فقط.
* استعلام `ALTER TABLE ... ATTACH` يُنسخ إلى النسخ المتماثلة. فهو يضيف البيانات إلى جميع النسخ المتماثلة. وتُضاف البيانات إلى إحدى النسخ المتماثلة من الدليل `detached`، وإلى النسخ الأخرى من النسخ المتماثلة المجاورة.

قبل التنزيل، يتحقق النظام مما إذا كان القسم موجودًا وما إذا كانت بنية الجدول متطابقة. ويُختار تلقائيًا أنسب نسخة متماثلة من بين النسخ المتماثلة السليمة.

على الرغم من أن اسم الاستعلام هو `ALTER TABLE`، فإنه لا يغيّر بنية الجدول ولا يغيّر على الفور البيانات المتاحة في الجدول.

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

ينقل الأقسام أو أجزاء البيانات إلى وحدة تخزين أو قرص آخر في الجداول التي تستخدم محرك `MergeTree`. راجع [استخدام عدة أجهزة تخزين كتلية لتخزين البيانات](/ar/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

استعلام `ALTER TABLE t MOVE`:

* غير مُكرَّر، لأن النسخ المتماثلة المختلفة قد تكون لها سياسات تخزين مختلفة.
* يعيد خطأ إذا لم يكن القرص أو وحدة التخزين المحددان مُهيّأين. ويعيد الاستعلام أيضًا خطأ إذا تعذّر تطبيق شروط نقل البيانات المحددة في سياسة التخزين.
* قد يعيد خطأ إذا كانت البيانات المطلوب نقلها قد نُقلت بالفعل بواسطة عملية في الخلفية، أو بواسطة استعلام `ALTER TABLE t MOVE` آخر متزامن، أو نتيجة دمج البيانات في الخلفية. ولا ينبغي للمستخدم اتخاذ أي إجراء إضافي في هذه الحالة.

مثال:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

يعدّل البيانات في القسم المحدد المطابقة لتعبير التصفية المحدد. ويُنفَّذ على شكل [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

البنية:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### مثال
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### راجع أيضًا
</div>

* [UPDATE](/ar/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

يحذف البيانات في القسم المحدد التي تطابق تعبير التصفية المحدد. ويُنَفَّذ ذلك على شكل [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

البنية:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### مثال
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

سيؤدي هذا إلى إعادة كتابة الأجزاء بالكامل من الصفر باستخدام جميع الإعدادات الجديدة. وهذا منطقي لأن الإعدادات على مستوى الجدول، مثل `use_const_adaptive_granularity`، لا تُطبَّق افتراضيًا إلا على الأجزاء التي تُكتَب حديثًا.

<div id="example">
  ### مثال
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also-1">
  ### انظر أيضًا
</div>

* [DELETE](/ar/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## كيفية تحديد تعبير قسم
</div>

يمكنك تحديد تعبير قسم في استعلامات `ALTER ... PARTITION` بطرق مختلفة:

* كقيمة من العمود `partition` في جدول `system.parts`. على سبيل المثال: `ALTER TABLE visits DETACH PARTITION 201901`.
* باستخدام الكلمة المفتاحية `ALL`. ولا يمكن استخدامها إلا مع DROP/DETACH/ATTACH/ATTACH FROM. على سبيل المثال: `ALTER TABLE visits ATTACH PARTITION ALL`.
* كـ tuple من تعبيرات أو ثوابت يطابق tuple مفاتيح تقسيم الجدول (من حيث الأنواع). وفي حالة مفتاح التقسيم المكوّن من عنصر واحد، يجب تغليف التعبير بالدالة `tuple (...)`. على سبيل المثال: `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
* باستخدام معرّف القسم. معرّف القسم هو معرّف نصي للقسم (مقروء للبشر إن أمكن) ويُستخدم كاسم للأقسام في نظام الملفات وفي ZooKeeper. ويجب تحديد معرّف القسم في العبارة `PARTITION ID`، بين علامتي اقتباس مفردتين. على سبيل المثال: `ALTER TABLE visits DETACH PARTITION ID '201901'`.
* في استعلامي [ALTER ATTACH PART](#attach-partitionpart) و[DROP DETACHED PART](#drop-detached-partitionpart)، لتحديد اسم جزء، استخدم قيمة حرفية نصية مأخوذة من العمود `name` في جدول [system.detached&#95;parts](/ar/operations/system-tables/detached_parts). على سبيل المثال: `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

يعتمد استخدام علامات الاقتباس عند تحديد القسم على نوع تعبير القسم. فعلى سبيل المثال، بالنسبة إلى النوع `String`، يجب تحديد اسمه بين علامتي اقتباس (`'`). أما بالنسبة إلى النوعين `Date` و`Int*`، فلا حاجة إلى علامات اقتباس.

تنطبق جميع القواعد المذكورة أعلاه أيضًا على استعلام [OPTIMIZE](/ar/sql-reference/statements/optimize.md). وإذا كنت بحاجة إلى تحديد القسم الوحيد عند تحسين جدول غير مُقسَّم، فاضبط التعبير `PARTITION tuple()`. على سبيل المثال:

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

يحدِّد `IN PARTITION` القسم الذي تُطبَّق عليه تعبيرات [UPDATE](/ar/sql-reference/statements/alter/update) أو [DELETE](/ar/sql-reference/statements/alter/delete) نتيجةً لاستعلام `ALTER TABLE`. ولا تُنشأ أجزاء جديدة إلا من القسم المحدَّد. وبهذه الطريقة، يساعد `IN PARTITION` على تقليل الحمل عندما يكون الجدول مقسَّمًا إلى عدد كبير من الأقسام، ولا تحتاج إلا إلى تحديث البيانات بشكل انتقائي.

تُعرَض أمثلة على استعلامات `ALTER ... PARTITION` في الاختبارين [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) و[`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).