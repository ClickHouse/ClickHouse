---
description: 'تُبسّط عبارة `DELETE` خفيفة الوزن عملية حذف البيانات من قاعدة البيانات.'
keywords: ['حذف']
sidebar_label: 'DELETE'
sidebar_position: 36
slug: /sql-reference/statements/delete
title: 'عبارة `DELETE` خفيفة الوزن'
doc_type: 'reference'
---

تُزيل عبارة `DELETE` خفيفة الوزن الصفوف من الجدول `[db.]table` التي تطابق التعبير `expr`. وهي متاحة فقط لعائلة محركات الجداول *MergeTree.

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

يُطلق عليه &quot;`DELETE` الخفيف&quot; تمييزًا له عن الأمر [ALTER TABLE ... DELETE](/ar/sql-reference/statements/alter/delete)، الذي يُعد عملية كثيفة الموارد.

<div id="examples">
  ## أمثلة
</div>

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

<div id="lightweight-delete-does-not-delete-data-immediately">
  ## لا يحذف `DELETE` خفيف الوزن البيانات فورًا
</div>

يُنفَّذ `DELETE` خفيف الوزن على هيئة [mutation](/ar/sql-reference/statements/alter#mutations) تضع علامة على الصفوف بأنها محذوفة، لكنه لا يحذفها فعليًا على الفور.

بشكل افتراضي، تنتظر عبارات `DELETE` حتى يكتمل تعليم الصفوف على أنها محذوفة قبل أن تعود بالنتيجة. وقد يستغرق ذلك وقتًا طويلًا إذا كان حجم البيانات كبيرًا. بدلًا من ذلك، يمكنك تشغيلها بشكل غير متزامن في الخلفية باستخدام الإعداد [`lightweight_deletes_sync`](/ar/operations/settings/settings#lightweight_deletes_sync). وإذا كان هذا الإعداد معطّلًا، فستعود عبارة `DELETE` بالنتيجة فورًا، لكن قد تظل البيانات مرئية للاستعلامات إلى أن تنتهي الـ mutation التي تعمل في الخلفية.

لا تؤدي الـ mutation إلى حذف الصفوف التي تم تعليمها على أنها محذوفة فعليًا؛ إذ لا يحدث ذلك إلا أثناء عملية الدمج التالية. ونتيجة لذلك، قد تظل البيانات، لفترة غير محددة، غير محذوفة فعليًا من التخزين، وإنما تكون فقط معلَّمة على أنها محذوفة.

إذا كنت بحاجة إلى ضمان حذف بياناتك من التخزين خلال مدة يمكن التنبؤ بها، ففكّر في استخدام إعداد الجدول [`min_age_to_force_merge_seconds`](/ar/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds). أو يمكنك استخدام الأمر [ALTER TABLE ... DELETE](/ar/sql-reference/statements/alter/delete). لاحظ أن حذف البيانات باستخدام `ALTER TABLE ... DELETE` قد يستهلك موارد كبيرة، لأنه يعيد إنشاء جميع الأجزاء المتأثرة.

<div id="deleting-large-amounts-of-data">
  ## حذف كميات كبيرة من البيانات
</div>

قد يؤثر حذف كميات كبيرة من البيانات سلبًا على أداء ClickHouse. إذا كنت تحاول حذف جميع الصفوف من جدول، ففكّر في استخدام الأمر [`TRUNCATE TABLE`](/ar/sql-reference/statements/truncate).

إذا كنت تتوقع عمليات حذف متكررة، ففكّر في استخدام [مفتاح تقسيم مخصّص](/ar/engines/table-engines/mergetree-family/custom-partitioning-key). بعد ذلك، يمكنك استخدام الأمر [`ALTER TABLE ... DROP PARTITION`](/ar/sql-reference/statements/alter/partition#drop-partitionpart) لحذف جميع الصفوف المرتبطة بهذا القسم بسرعة.

<div id="limitations-of-lightweight-delete">
  ## قيود `DELETE` خفيف الوزن
</div>

<div id="lightweight-deletes-with-projections">
  ### عمليات `DELETE` الخفيفة الوزن مع الإسقاطات
</div>

بشكل افتراضي، لا يعمل `DELETE` مع الجداول التي تحتوي على إسقاطات. ويرجع ذلك إلى أن الصفوف في الإسقاط قد تتأثر بعملية `DELETE`. لكن يتوفر [إعداد MergeTree](/ar/operations/settings/merge-tree-settings) باسم `lightweight_mutation_projection_mode` لتغيير هذا السلوك.

<div id="performance-considerations-when-using-lightweight-delete">
  ## اعتبارات الأداء عند استخدام عبارة `DELETE` خفيف الوزن
</div>

**قد يؤثر حذف كميات كبيرة من البيانات باستخدام عبارة `DELETE` خفيف الوزن سلبًا على أداء استعلامات SELECT.**

قد تؤثر العوامل التالية أيضًا سلبًا على أداء `DELETE` خفيف الوزن:

* وجود شرط `WHERE` معقد في استعلام `DELETE`.
* إذا كان طابور عمليات mutation ممتلئًا بعدد كبير من عمليات mutation الأخرى، فقد يؤدي ذلك إلى مشكلات في الأداء، لأن جميع عمليات mutation على الجدول تُنفَّذ تسلسليًا.
* احتواء الجدول المتأثر على عدد كبير جدًا من أجزاء البيانات.
* وجود كمية كبيرة من البيانات في الأجزاء المدمجة. ففي الجزء المدمج، تُخزَّن جميع الأعمدة في ملف واحد.

<div id="delete-permissions">
  ## أذونات الحذف
</div>

يتطلب `DELETE` امتياز `ALTER DELETE`. لتفعيل عبارات `DELETE` على جدول معيّن لمستخدم محدد، شغّل الأمر التالي:

```sql
GRANT ALTER DELETE ON db.table to username;
```

<div id="how-lightweight-deletes-work-internally-in-clickhouse">
  ## كيف تعمل عمليات DELETE خفيفة الوزن داخليًا في ClickHouse
</div>

1. **يُطبَّق &quot;قناع&quot; على الصفوف المتأثرة**

   عند تنفيذ استعلام `DELETE FROM table ...`، يحفظ ClickHouse قناعًا يُشار فيه إلى كل صف على أنه إما &quot;موجود&quot; أو &quot;محذوف&quot;. وتُستبعَد الصفوف &quot;المحذوفة&quot; من الاستعلامات اللاحقة. ومع ذلك، لا تُزال الصفوف فعليًا إلا لاحقًا من خلال عمليات الدمج اللاحقة. وتكون كتابة هذا القناع أخف بكثير مما يحدث في استعلام `ALTER TABLE ... DELETE`.

   يُنفَّذ هذا القناع على شكل عمود نظامي مخفي باسم `_row_exists` يخزّن القيمة `True` لكل الصفوف المرئية و`False` للصفوف المحذوفة. ولا يظهر هذا العمود في أي جزء إلا إذا كانت بعض الصفوف في ذلك الجزء قد حُذفت. ولا يوجد هذا العمود إذا كانت جميع القيم في الجزء تساوي `True`.

2. **تُحوَّل استعلامات `SELECT` بحيث تتضمن القناع**

   عند استخدام عمود مطبَّق عليه القناع في استعلام، يُوسَّع استعلام `SELECT ... FROM table WHERE condition` داخليًا بإضافة الشرط على `_row_exists`، ويتحوّل إلى:

   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```

   وعند التنفيذ، يُقرأ العمود `_row_exists` لتحديد الصفوف التي يجب عدم إرجاعها. وإذا كان هناك عدد كبير من الصفوف المحذوفة، يمكن لـ ClickHouse تحديد وحدات `granule` التي يمكن تخطيها بالكامل عند قراءة بقية الأعمدة.

3. **تُحوَّل استعلامات `DELETE` إلى استعلامات `ALTER TABLE ... UPDATE`**

   يُترجم `DELETE FROM table WHERE condition` إلى عملية `mutation` بالشكل `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition`.

   داخليًا، تُنفَّذ هذه العملية على خطوتين:

   1. يُنفَّذ الأمر `SELECT count() FROM table WHERE condition` لكل جزء على حدة لتحديد ما إذا كان هذا الجزء متأثرًا.

   2. وبناءً على الأوامر أعلاه، تُطبَّق `mutation` على الأجزاء المتأثرة، وتُنشأ روابط صلبة للأجزاء غير المتأثرة. وفي حالة الأجزاء العريضة، يُحدَّث العمود `_row_exists` لكل صف، بينما تُنشأ روابط صلبة لملفات جميع الأعمدة الأخرى. أما في الأجزاء المدمجة، فتُعاد كتابة جميع الأعمدة لأنها مخزنة معًا في ملف واحد.

   ومن الخطوات أعلاه، يتضح أن استخدام `DELETE` خفيف الوزن بأسلوب القناع يحسّن الأداء مقارنةً بـ `ALTER TABLE ... DELETE` التقليدي، لأنه لا يعيد كتابة ملفات جميع الأعمدة في الأجزاء المتأثرة.

<div id="related-content">
  ## محتوى ذي صلة
</div>

* مدونة: [التعامل مع التحديثات وعمليات الحذف في ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)