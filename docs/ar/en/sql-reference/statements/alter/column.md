---
description: 'توثيق العمود'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: 'تعديلات الأعمدة'
doc_type: 'reference'
---

مجموعة من الاستعلامات التي تتيح تعديل بنية الجدول.

الصيغة:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

في الاستعلام، حدِّد قائمة تضم إجراءً واحدًا أو أكثر، مفصولة بفواصل.
يمثّل كل إجراء عمليةً تُجرى على عمود.

الإجراءات التالية مدعومة:

* [ADD COLUMN](#add-column) — يضيف عمودًا جديدًا إلى الجدول.
* [DROP COLUMN](#drop-column) — يحذف العمود.
* [RENAME COLUMN](#rename-column) — يعيد تسمية عمود موجود.
* [CLEAR COLUMN](#clear-column) — يعيد تعيين قيم العمود.
* [COMMENT COLUMN](#comment-column) — يضيف تعليقًا نصيًا إلى العمود.
* [MODIFY COLUMN](#modify-column) — يغيّر نوع العمود، والتعبير الافتراضي، وTTL، وإعدادات العمود.
* [MODIFY COLUMN REMOVE](#modify-column-remove) — يزيل إحدى خصائص العمود.
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - يغيّر إعدادات العمود.
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - يعيد تعيين إعدادات العمود.
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - يضيف قيمًا جديدة إلى Enum.
* [MATERIALIZE COLUMN](#materialize-column) — يُخزِّن العمود تخزينًا ماديًا في الأجزاء التي يكون فيها العمود مفقودًا.
  تُوصَف هذه الإجراءات بالتفصيل أدناه.

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

يضيف عمودًا جديدًا إلى الجدول بالاسم `name` والنوع `type` و[`codec`](../create/table.md/#column_compression_codec) و`default_expr` المحددة (راجع قسم [التعبيرات الافتراضية](/ar/sql-reference/statements/create/table#default_values)).

إذا تضمّنت العبارة `IF NOT EXISTS`، فلن يُرجع الاستعلام خطأً إذا كان العمود موجودًا بالفعل. وإذا حدّدت `AFTER name_after` (اسم عمود آخر)، فسيُضاف العمود بعد العمود المحدد في قائمة أعمدة الجدول. وإذا أردت إضافة عمود إلى بداية الجدول، فاستخدم العبارة `FIRST`. وإلا فسيُضاف العمود إلى نهاية الجدول. وفي حالة سلسلة من الإجراءات، يمكن أن يكون `name_after` اسم عمود أُضيف في أحد الإجراءات السابقة.

إن إضافة عمود لا تغيّر سوى بنية الجدول، من دون تنفيذ أي عمليات على البيانات. ولا تظهر البيانات على القرص بعد `ALTER`. وإذا كانت بيانات عمود ما مفقودة عند القراءة من الجدول، فستُملأ بالقيم الافتراضية (بتنفيذ التعبير الافتراضي إن وُجد، أو باستخدام الأصفار أو السلاسل الفارغة). ويظهر العمود على القرص بعد دمج أجزاء البيانات (راجع [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree.md)).

يتيح هذا النهج إكمال استعلام `ALTER` فورًا، من دون زيادة حجم البيانات القديمة.

مثال:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

يحذف العمود الذي يحمل الاسم `name`. إذا تم تحديد العبارة `IF EXISTS`، فلن يُرجع الاستعلام خطأً إذا لم يكن العمود موجودًا.

يحذف البيانات من نظام الملفات. ونظرًا لأن هذا يحذف ملفات كاملة، يكتمل الاستعلام تقريبًا على الفور.

:::tip
لا يمكنك حذف عمود إذا كان مُشارًا إليه في [عرض مُجسَّد](/ar/sql-reference/statements/create/view). وإلا، فسيُرجع خطأً.
:::

مثال:

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

يعيد تسمية العمود `name` إلى `new_name`. إذا تم تحديد عبارة `IF EXISTS`، فلن يُرجع الاستعلام خطأً إذا لم يكن العمود موجودًا. ونظرًا لأن إعادة التسمية لا تمسّ البيانات الفعلية، يُنفَّذ الاستعلام بشكل شبه فوري.

**ملاحظة**: لا يمكن إعادة تسمية الأعمدة المحددة في تعبير المفتاح للجدول (سواء باستخدام `ORDER BY` أو `PRIMARY KEY`). وستؤدي محاولة تغيير هذه الأعمدة إلى ظهور `SQL Error [524]`.

مثال:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

يعيد تعيين جميع البيانات في عمود لقسم محدد. اقرأ المزيد حول تحديد اسم القسم في قسم [كيفية تعيين تعبير القسم](../alter/partition.md/#how-to-set-partition-expression).

إذا تم تحديد العبارة `IF EXISTS`، فلن يُرجع الاستعلام خطأً إذا لم يكن العمود موجودًا.

مثال:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

يضيف تعليقًا إلى العمود. إذا تم تحديد العبارة `IF EXISTS`، فلن يُرجع الاستعلام خطأً إذا كان العمود غير موجود.

يمكن أن يتضمن كل عمود تعليقًا واحدًا. وإذا كان هناك تعليق موجود بالفعل للعمود، فسيحلّ التعليق الجديد محلّ التعليق السابق.

تُخزَّن التعليقات في العمود `comment_expression` الذي يُرجعه استعلام [DESCRIBE TABLE](/ar/sql-reference/statements/describe-table.md).

مثال:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

يغيّر هذا الاستعلام خصائص العمود `name` التالية:

* النوع

* التعبير الافتراضي

* ترميز الضغط

* TTL

* الإعدادات على مستوى العمود

* قيم Enum لأنواع Enum/Enum8/Enum16

للاطلاع على أمثلة على تعديل CODECS ضغط الأعمدة، راجع [ترميزات ضغط الأعمدة](../create/table.md/#column_compression_codec).

للاطلاع على أمثلة على تعديل TTL للأعمدة، راجع [TTL العمود](/ar/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl).

للاطلاع على أمثلة على تعديل الإعدادات على مستوى العمود، راجع [الإعدادات على مستوى العمود](/ar/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings).

إذا جرى تحديد العبارة `IF EXISTS`، فلن يُرجع الاستعلام خطأً إذا لم يكن العمود موجودًا.

عند تغيير النوع، تُحوَّل القيم كما لو أن دوال [toType](/ar/sql-reference/functions/type-conversion-functions.md) طُبِّقت عليها. أما إذا جرى تغيير التعبير الافتراضي فقط، فلن ينفّذ الاستعلام أي إجراء معقد، وسيكتمل تقريبًا على الفور.

مثال:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

يُعد تغيير نوع العمود الإجراء المعقد الوحيد، إذ يغيّر محتويات ملفات البيانات. وفي الجداول الكبيرة، قد يستغرق ذلك وقتًا طويلًا.

يمكن للاستعلام أيضًا تغيير ترتيب الأعمدة باستخدام العبارة `FIRST | AFTER`، راجع وصف [ADD COLUMN](#add-column)، لكن في هذه الحالة يكون نوع العمود إلزاميًا.

مثال:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

استعلام `ALTER` ذريّ. وبالنسبة إلى جداول MergeTree، فهو أيضًا خالٍ من الأقفال.

يُكرَّر استعلام `ALTER` الخاص بتغيير الأعمدة. تُحفَظ التعليمات في ZooKeeper، ثم تطبّقها كل نسخة متماثلة. تُنفَّذ جميع استعلامات `ALTER` بالترتيب نفسه. وينتظر الاستعلام حتى تكتمل الإجراءات المناسبة على النسخ المتماثلة الأخرى. ومع ذلك، يمكن مقاطعة استعلام تغيير الأعمدة في جدول مكرّر، وستُنفَّذ جميع الإجراءات بشكل غير متزامن.

:::note
يُرجى توخّي الحذر عند تغيير عمود Nullable إلى Non-Nullable. تأكّد من أنه لا يحتوي على أي قيم NULL، وإلا فسيتسبّب ذلك في مشكلات عند القراءة منه. وفي هذه الحالة، يكون الحل البديل هو تنفيذ Kill على الـ mutation وإعادة العمود إلى النوع Nullable.
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

يزيل إحدى خصائص العمود: `DEFAULT`، `ALIAS`، `MATERIALIZED`، `CODEC`، `COMMENT`، `TTL`، `SETTINGS`.

الصيغة:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**مثال**

أزِل TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**انظر أيضًا**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

قم بتعديل إعداد عمود.

الصيغة:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**مثال**

عدّل قيمة `max_compress_block_size` للعمود إلى `1MB`:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

يعيد تعيين إعداد العمود، كما يزيل أيضًا تصريح الإعداد من تعبير العمود في استعلام CREATE الخاص بالجدول.

الصيغة:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**مثال**

أعِد تعيين إعداد العمود `max_compress_block_size` إلى قيمته الافتراضية:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

يضيف قيماً جديدة إلى عمود من النوع `Enum` أو `Enum8` أو `Enum16` أو `Nullable(Enum)` أو `Nullable(Enum8)` أو `Nullable(Enum16)`

الصيغة:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**مثال**

أضِف قيمتين إلى العمود `enum_column_name`:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

يقوم بتمثيل عمود ماديًا باستخدام تعبير قيمة `DEFAULT` أو `MATERIALIZED`. عند إضافة عمود مادي باستخدام `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`، لا تُستكمل تلقائيًا الصفوف الموجودة التي لا تحتوي على قيم مادية. يمكن استخدام عبارة `MATERIALIZE COLUMN` لإعادة كتابة بيانات الأعمدة الموجودة بعد إضافة تعبير `DEFAULT` أو `MATERIALIZED` أو تحديثه (إذ لا يؤدي ذلك إلا إلى تحديث البيانات الوصفية دون تغيير البيانات الموجودة). لاحظ أن تمثيل عمود ضمن مفتاح الفرز ماديًا عملية غير صالحة، لأنه قد يفسد ترتيب الفرز.
وهي مُنفَّذة على هيئة [mutation](/ar/sql-reference/statements/alter/index.md#mutations).

بالنسبة إلى الأعمدة التي لها تعبير قيمة `MATERIALIZED` جديد أو محدَّث، يُعاد كتابة جميع الصفوف الموجودة.

بالنسبة إلى الأعمدة التي لها تعبير قيمة `DEFAULT` جديد أو محدَّث، يعتمد السلوك على إصدار ClickHouse:

* في ClickHouse &lt; v24.2، يُعاد كتابة جميع الصفوف الموجودة.
* يميّز ClickHouse &gt;= v24.2 بين ما إذا كانت قيمة صف في عمود ذي تعبير قيمة `DEFAULT` قد حُدِّدت صراحةً عند إدراج الصف، أو كانت محسوبة من تعبير قيمة `DEFAULT`. فإذا كانت القيمة قد حُدِّدت صراحةً، أبقاها ClickHouse كما هي. أما إذا كانت محسوبة، فيغيّرها ClickHouse إلى تعبير قيمة `MATERIALIZED` الجديد أو المحدَّث.

صيغة:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* إذا حددت PARTITION، فسيُخزَّن عمود فعليًا للقسم المحدد فقط.

**مثال**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**انظر أيضًا**

* [MATERIALIZED](/ar/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## القيود
</div>

يتيح الاستعلام `ALTER` إنشاء عناصر منفصلة (أعمدة) وحذفها داخل بُنى البيانات المتداخلة، لكنه لا يدعم إنشاء بُنى بيانات متداخلة كاملة أو حذفها. ولإضافة بنية بيانات متداخلة، يمكنك إضافة أعمدة باسم مثل `name.nested_name` وبالنوع `Array(T)`. وتكافئ بنية البيانات المتداخلة عدة أعمدة مصفوفة تشترك أسماؤها في البادئة نفسها قبل النقطة.

دعم إعادة تسمية الأعمدة التي تحتوي أسماؤها على نقاط هو دعم جزئي. فالنقاط محجوزة للوصول إلى الأعمدة الفرعية من [Nested](/ar/sql-reference/data-types/nested-data-structures/nested)، لذلك يجب أن تظل البادئة (اسم الأصل) كما هي. ولا يمكن تغيير سوى اللاحقة (اسم العمود الفرعي). على سبيل المثال، يمكن إعادة تسمية `a.b` إلى `a.c`، لكن لا يُسمح بإعادة تسمية `a.b` إلى `b.d` لأن ذلك يغيّر بادئة الأصل في Nested.

لا يوجد دعم لحذف الأعمدة الموجودة في المفتاح الأساسي أو مفتاح أخذ العينات (أي الأعمدة المستخدمة في تعبير `ENGINE`). ولا يمكن تغيير نوع الأعمدة المضمّنة في المفتاح الأساسي إلا إذا كان هذا التغيير لا يؤدي إلى تعديل البيانات (على سبيل المثال، يُسمح بإضافة قيم إلى Enum أو تغيير النوع من `DateTime` إلى `UInt32`).

إذا لم يكن الاستعلام `ALTER` كافيًا لإجراء تغييرات الجدول التي تحتاج إليها، فيمكنك إنشاء جدول جديد، ونسخ البيانات إليه باستخدام الاستعلام [INSERT SELECT](/ar/sql-reference/statements/insert-into.md/#inserting-the-results-of-select)، ثم تبديل الجدولين باستخدام الاستعلام [RENAME](/ar/sql-reference/statements/rename.md/#rename-table) وحذف الجدول القديم.

يحظر الاستعلام `ALTER` جميع عمليات القراءة والكتابة على الجدول. وبعبارة أخرى، إذا كان استعلام `SELECT` طويلًا قيد التشغيل وقت تنفيذ الاستعلام `ALTER`، فسينتظر الاستعلام `ALTER` حتى يكتمل. وفي الوقت نفسه، ستنتظر جميع الاستعلامات الجديدة على الجدول نفسه طوال مدة تشغيل هذا `ALTER`.

بالنسبة إلى الجداول التي لا تخزّن البيانات بنفسها (مثل [Merge](/ar/sql-reference/statements/alter/index.md) و[Distributed](/ar/sql-reference/statements/alter/index.md))، فإن `ALTER` يغيّر فقط بنية الجدول، ولا يغيّر بنية الجداول التابعة. على سبيل المثال، عند تشغيل `ALTER` على جدول `Distributed`، ستحتاج أيضًا إلى تشغيل `ALTER` على الجداول الموجودة على جميع الخوادم البعيدة.