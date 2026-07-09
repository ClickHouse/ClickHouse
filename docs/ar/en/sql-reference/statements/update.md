---
description: 'تُسهّل التحديثات خفيفة الوزن عملية تحديث البيانات في قاعدة البيانات باستخدام أجزاء التصحيح.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: 'عبارة `UPDATE` خفيفة الوزن'
doc_type: 'مرجع'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
التحديثات خفيفة الوزن لا تزال حاليًا في المرحلة التجريبية.
إذا واجهت أي مشكلات، يُرجى فتح بلاغ في [مستودع ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

تُحدِّث عبارة `UPDATE` الخفيفة الصفوف في الجدول `[db.]table` التي تطابق التعبير `filter_expr`.
ويُطلق عليها اسم &quot;التحديث الخفيف&quot; تمييزًا لها عن الاستعلام [`ALTER TABLE ... UPDATE`](/ar/sql-reference/statements/alter/update)، وهو إجراء ثقيل يعيد كتابة أعمدة كاملة في أجزاء البيانات.
وهي متاحة فقط لعائلة محركات الجداول [`MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree).

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

يجب أن يكون `filter_expr` من النوع `UInt8`. يحدّث هذا الاستعلام قيم الأعمدة المحددة لتصبح قيم التعبيرات المقابلة في الصفوف التي تكون فيها قيمة `filter_expr` غير صفرية.
تُحوَّل القيم إلى نوع العمود باستخدام المعامل `CAST`. لا يُدعم تحديث الأعمدة المستخدمة في حساب المفتاح الأساسي أو مفتاح التقسيم.

<div id="examples">
  ## أمثلة
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## لا تُحدِّث التحديثات خفيفة الوزن البيانات فورًا
</div>

يُنفَّذ `UPDATE` خفيف الوزن باستخدام **أجزاء التصحيح**، وهي نوع خاص من أجزاء البيانات لا يحتوي إلا على الأعمدة والصفوف المحدَّثة.
ينشئ `UPDATE` خفيف الوزن أجزاء تصحيح، لكنه لا يعدِّل البيانات الأصلية فعليًا في التخزين على الفور.
تشبه عملية التحديث استعلام `INSERT ... SELECT ...`، لكن استعلام `UPDATE` ينتظر حتى يكتمل إنشاء جزء التصحيح قبل أن ينتهي.

تكون القيم المحدَّثة:

* **مرئية فورًا** في استعلامات `SELECT` من خلال تطبيق التصحيحات
* **تُطبَّق فعليًا** فقط أثناء عمليات الدمج وعمليات التعديل اللاحقة
* **تُزال تلقائيًا** بمجرد أن تُطبَّق التصحيحات فعليًا على جميع الأجزاء النشطة

<div id="lightweight-update-requirements">
  ## متطلبات التحديثات الخفيفة
</div>

التحديثات الخفيفة مدعومة في محركات [`MergeTree`](/ar/engines/table-engines/mergetree-family/mergetree)، و[`ReplacingMergeTree`](/ar/engines/table-engines/mergetree-family/replacingmergetree)، و[`CollapsingMergeTree`](/ar/engines/table-engines/mergetree-family/collapsingmergetree)، و[`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree)، وكذلك في إصداراتها [`Replicated`](/ar/engines/table-engines/mergetree-family/replication.md) و[`Shared`](/ar/cloud/reference/shared-merge-tree).

لاستخدام التحديثات الخفيفة، يجب تمكين materialization للعمودين `_block_number` و`_block_offset` باستخدام table settings [`enable_block_number_column`](/ar/operations/settings/merge-tree-settings#enable_block_number_column) و[`enable_block_offset_column`](/ar/operations/settings/merge-tree-settings#enable_block_offset_column).

<div id="lightweight-delete">
  ## عمليات الحذف خفيفة الوزن
</div>

يمكن تشغيل استعلام [lightweight `DELETE`](/ar/sql-reference/statements/delete) على أنه `UPDATE` خفيف الوزن بدلًا من mutation من نوع `ALTER UPDATE`. ويُتحكَّم في تنفيذ `lightweight DELETE` من خلال الإعداد [`lightweight_delete_mode`](/ar/operations/settings/settings#lightweight_delete_mode).

<div id="performance-considerations">
  ## اعتبارات الأداء
</div>

**مزايا التحديثات الخفيفة:**

* زمن استجابة التحديث مماثل لزمن استجابة الاستعلام `INSERT ... SELECT ...`
* لا تُكتب إلا الأعمدة والقيم المحدَّثة، وليس الأعمدة كاملةً داخل أجزاء البيانات
* لا حاجة إلى انتظار اكتمال عمليات الدمج/التعديلات الجارية حاليًا، لذلك يكون زمن استجابة التحديث قابلًا للتنبؤ
* يمكن تنفيذ التحديثات الخفيفة بالتوازي

**التأثيرات المحتملة على الأداء:**

* تضيف عبئًا إضافيًا إلى استعلامات `SELECT` التي تحتاج إلى تطبيق التصحيحات
* لن تُستخدم [فهارس التخطي](/ar/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) للأعمدة في أجزاء البيانات التي توجد بها تصحيحات يجب تطبيقها. ولن تُستخدم [الإسقاطات](/ar/engines/table-engines/mergetree-family/mergetree.md/#projections) إذا كانت هناك أجزاء تصحيح للجدول، بما في ذلك أجزاء البيانات التي لا توجد بها تصحيحات يجب تطبيقها.
* قد تؤدي التحديثات الصغيرة المتكررة جدًا إلى ظهور خطأ &quot;عدد الأجزاء كبير جدًا&quot;. ويُنصح بتجميع عدة تحديثات في استعلام واحد، على سبيل المثال بوضع معرّفات التحديث في عبارة `IN` واحدة داخل عبارة `WHERE`
* صُممت التحديثات الخفيفة لتحديث عدد صغير من الصفوف (حتى نحو 10% من الجدول). وإذا كنت بحاجة إلى تحديث عدد أكبر، فيُوصى باستخدام mutation [`ALTER TABLE ... UPDATE`](/ar/sql-reference/statements/alter/update)

<div id="concurrent-operations">
  ## العمليات المتزامنة
</div>

لا تنتظر التحديثات الخفيفة اكتمال عمليات الدمج/الـmutations الجارية حاليًا، على عكس الـmutations الثقيلة.
يُضبط اتساق التحديثات الخفيفة المتزامنة من خلال الإعدادات [`update_sequential_consistency`](/ar/operations/settings/settings#update_sequential_consistency) و[`update_parallel_mode`](/ar/operations/settings/settings#update_parallel_mode).

<div id="update-permissions">
  ## أذونات التحديث
</div>

يتطلب `UPDATE` امتياز `ALTER UPDATE`. لتمكين عبارات `UPDATE` على جدول معيّن لمستخدم محدد، شغّل:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## تفاصيل التنفيذ
</div>

أجزاء التصحيح مماثلة للأجزاء العادية، لكنها لا تحتوي إلا على الأعمدة المُحدَّثة وعدة أعمدة نظام:

* `_part` - اسم الجزء الأصلي
* `_part_offset` - رقم الصف في الجزء الأصلي
* `_block_number` - رقم الـ block الخاص بالصف في الجزء الأصلي
* `_block_offset` - إزاحة الـ block الخاصة بالصف في الجزء الأصلي
* `_data_version` - إصدار البيانات للبيانات المُحدَّثة (رقم الـ block المُخصَّص لاستعلام `UPDATE`)

في المتوسط، ينتج عن ذلك تكلفة إضافية تبلغ نحو 40 بايتًا (بيانات غير مضغوطة) لكل صف مُحدَّث في أجزاء التصحيح.
وتساعد أعمدة النظام في العثور على الصفوف داخل الجزء الأصلي التي يجب تحديثها.
وترتبط أعمدة النظام بـ [الأعمدة الافتراضية](/ar/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) في الجزء الأصلي، والتي تُضاف عند القراءة إذا كان ينبغي تطبيق أجزاء التصحيح.
وتُرتَّب أجزاء التصحيح حسب `_part` و `_part_offset`.

تنتمي أجزاء التصحيح إلى partitionات مختلفة عن الجزء الأصلي.
ويكون معرّف partition لجزء التصحيح هو `patch-<hash of column names in patch part>-<original_partition_id>`.
ولذلك تُخزَّن أجزاء التصحيح ذات الأعمدة المختلفة في partitionات مختلفة.
فعلى سبيل المثال، ستؤدي التحديثات الثلاثة `SET x = 1 WHERE <cond>` و `SET y = 1 WHERE <cond>` و `SET x = 1, y = 1 WHERE <cond>` إلى إنشاء ثلاثة أجزاء تصحيح في ثلاث partitionات مختلفة.

يمكن دمج أجزاء التصحيح مع بعضها لتقليل عدد التصحيحات المُطبَّقة على استعلامات `SELECT` وتقليل التكلفة الإضافية. ويستخدم دمج أجزاء التصحيح خوارزمية الدمج [الاستبدالية](/ar/engines/table-engines/mergetree-family/replacingmergetree) مع `_data_version` باعتباره عمود إصدار.
ولذلك تحتفظ أجزاء التصحيح دائمًا بأحدث إصدار لكل صف مُحدَّث في الجزء.

لا تنتظر التحديثات خفيفة الوزن حتى تنتهي عمليات الدمج وعمليات mutations الجارية حاليًا، بل تستخدم دائمًا لقطة حالية من أجزاء البيانات لتنفيذ التحديث وإنتاج جزء تصحيح.
وبسبب ذلك، قد تظهر حالتان عند تطبيق أجزاء التصحيح.

فعلى سبيل المثال، إذا قرأنا الجزء `A`، فسنحتاج إلى تطبيق جزء التصحيح `X`:

* إذا كان `X` يحتوي على الجزء `A` نفسه. يحدث ذلك إذا لم يكن `A` مشاركًا في عملية دمج عند تنفيذ `UPDATE`.
* إذا كان `X` يحتوي على الجزأين `B` و `C`، اللذين يغطيهما الجزء `A`. يحدث ذلك إذا كانت هناك عملية دمج (`B`, `C`) -&gt; `A` قيد التشغيل عند تنفيذ `UPDATE`.

ولهاتين الحالتين، توجد طريقتان لتطبيق أجزاء التصحيح على الترتيب:

* استخدام الدمج حسب الأعمدة المُرتَّبة `_part`, `_part_offset`.
* استخدام join حسب العمودين `_block_number`, `_block_offset`.

ويكون وضع join أبطأ ويتطلب ذاكرة أكبر من وضع الدمج، لكنه يُستخدم بوتيرة أقل.

<div id="related-content">
  ## محتوى ذي صلة
</div>

* [`ALTER UPDATE`](/ar/sql-reference/statements/alter/update) - عمليات `UPDATE` كثيفة
* [`DELETE` خفيف الوزن](/ar/sql-reference/statements/delete) - عمليات `DELETE` خفيفة الوزن
* [`APPLY PATCHES`](/ar/sql-reference/statements/alter/apply-patches) - فرض التخزين المادي للتصحيحات في أجزاء البيانات (عملية mutation)