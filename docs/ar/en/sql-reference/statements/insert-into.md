---
description: 'توثيق لعبارة INSERT INTO'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'عبارة INSERT INTO'
doc_type: 'reference'
---

يُدرج بيانات في جدول.

**الصيغة**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

يمكنك تحديد قائمة بالأعمدة المراد إدراجها باستخدام `(c1, c2, c3)`. ويمكنك أيضًا استخدام تعبير باستخدام [مطابِق](../../sql-reference/statements/select/index.md#asterisk) الأعمدة مثل `*` و/أو [مُعدِّلات](../../sql-reference/statements/select/index.md#select-modifiers) مثل [APPLY](/ar/sql-reference/statements/select/apply-modifier) و[EXCEPT](/ar/sql-reference/statements/select/except-modifier) و[REPLACE](/ar/sql-reference/statements/select/replace-modifier).

على سبيل المثال، لنفترض الجدول التالي:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

إذا كنت تريد إدراج البيانات في جميع الأعمدة، باستثناء العمود `b`، فيمكنك فعل ذلك باستخدام الكلمة المفتاحية `EXCEPT`. واستنادًا إلى الصياغة المذكورة أعلاه، ستحتاج إلى التأكد من إدراج عدد من القيم (`VALUES (v11, v13)`) يساوي عدد الأعمدة التي تحددها (`(c1, c3)`) :

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

في هذا المثال، نرى أن الصف المُدرَج الثاني أُسنِدت إلى العمودين `a` و`c` فيه القيم الممرَّرة، بينما أُسنِدت إلى `b` القيمة الافتراضية. ومن الممكن أيضًا استخدام الكلمة المفتاحية `DEFAULT` لإدراج القيم الافتراضية:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

إذا كانت قائمة الأعمدة لا تتضمن جميع الأعمدة الموجودة، فستُملأ الأعمدة المتبقية بما يلي:

* القيم المحسوبة من تعبيرات `DEFAULT` المحددة في تعريف الجدول.
* الأصفار والسلاسل النصية الفارغة، إذا لم تكن تعبيرات `DEFAULT` معرّفة.

يمكن تمرير البيانات إلى INSERT بأي [تنسيق](/ar/sql-reference/formats) يدعمه ClickHouse. ويجب تحديد التنسيق صراحةً في الاستعلام:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

على سبيل المثال، فإن صيغة الاستعلام التالية مماثلة للنسخة الأساسية من `INSERT ... VALUES`:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

يزيل ClickHouse جميع المسافات ومحرف تغذية سطر واحدًا (إن وُجد) قبل البيانات. عند تكوين استعلام، نوصي بوضع البيانات في سطر جديد بعد عوامل الاستعلام، وهذا مهم إذا كانت البيانات تبدأ بمسافات.

مثال:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

يمكنك إدراج البيانات بشكل مستقل عن الاستعلام باستخدام [عميل سطر الأوامر](/ar/operations/utilities/clickhouse-local) أو [واجهة HTTP](/ar/interfaces/http).

:::note
إذا أردت تحديد `SETTINGS` لاستعلام `INSERT`، فيجب فعل ذلك *قبل* عبارة `FORMAT` لأن كل ما يأتي بعد `FORMAT format_name` يُعامل على أنه بيانات. على سبيل المثال:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## القيود
</div>

إذا كان للـ [جدول](../../sql-reference/statements/create/table.md#constraints) [قيود](../../sql-reference/statements/create/table.md#constraints)، فسيتم التحقق من تعبيراتها لكل صف من البيانات المُدرجة. وإذا لم يُستوفَ أيٌّ من هذه القيود، فسيُرجع الخادم استثناءً يتضمن اسم القيد وتعبيره، وسيتم إيقاف الاستعلام.

<div id="data-type-validation">
  ## التحقق من صحة نوع البيانات
</div>

يتحقق ClickHouse من أنواع البيانات المسموح بها (التي تتحكم فيها إعدادات مثل `enable_time_time64_type` و`allow_suspicious_low_cardinality_types` و`allow_suspicious_fixed_string_types` وغيرها) فقط عند إنشاء الجدول (`CREATE TABLE`) وتعديل المخطط (`ALTER TABLE`)، وليس أثناء `INSERT`.

وهذا يعني أنه إذا كان هناك جدول موجود مسبقًا يحتوي على نوع بيانات غير مسموح به، فلا يزال بالإمكان إدراج البيانات فيه حتى عند تعطيل الإعداد المقابل على الخادم. وهذا مقصود في التصميم — فبمجرد إنشاء الجدول، يجب ألا تُمنع عمليات الإدراج بسبب الإعدادات التي تتحكم في إنشاء الأنواع.

على سبيل المثال:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
ونتيجةً لذلك، يمكن لعميل يعمل بإصدار أحدث (حيث يكون أحد الإعدادات مفعّلًا افتراضيًا) أن يدرج بيانات ذات أنواع غير مسموح بها إلى خادم يعمل بإصدار أقدم (حيث يكون هذا الإعداد معطّلًا)، ما دام الجدول الهدف يحتوي بالفعل على أنواع الأعمدة المقابلة. ويُفرَض التحقق على مستوى DDL، وليس على مستوى DML.
:::

<div id="inserting-the-results-of-select">
  ## إدراج نتائج SELECT
</div>

**الصيغة**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

تُطابَق الأعمدة وفقًا لمواضعها في عبارة `SELECT`. ومع ذلك، قد تختلف أسماؤها بين تعبير `SELECT` والجدول المستخدم في `INSERT`. وإذا لزم الأمر، يُجرى تحويل النوع.

لا يتيح أيّ من تنسيقات البيانات، باستثناء تنسيق Values، تعيين قيم إلى تعبيرات مثل `now()` و`1 + 2` وما إلى ذلك. ويتيح تنسيق Values استخدامًا محدودًا للتعبيرات، لكن لا يُنصح بذلك، لأن تنفيذها في هذه الحالة يعتمد على شيفرة غير فعّالة.

الاستعلامات الأخرى لتعديل أجزاء البيانات غير مدعومة: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
ومع ذلك، يمكنك حذف البيانات القديمة باستخدام `ALTER TABLE ... DROP PARTITION`.

يجب تحديد عبارة `FORMAT` في نهاية الاستعلام إذا كانت عبارة `SELECT` تحتوي على دالة جدولية [input()](../../sql-reference/table-functions/input.md).

لإدراج قيمة افتراضية بدلًا من `NULL` في عمود ذي نوع بيانات غير قابل لـ NULL، فعِّل الإعداد [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default).

يدعم `INSERT` أيضًا CTE (تعبير الجدول الشائع). على سبيل المثال، التعليمتان التاليتان متكافئتان:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## إدراج البيانات من ملف
</div>

**الصيغة**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

استخدم الصياغة أعلاه لإدراج البيانات من ملف أو ملفات محفوظة على جهة **العميل**. `file_name` و`type` هما قيمتان حرفيتان نصيتان. يجب تحديد [تنسيق](../../interfaces/formats.md) ملف الإدخال في بند `FORMAT`.

الملفات المضغوطة مدعومة. يُستدل على نوع الضغط من امتداد اسم الملف، أو يمكن تحديده صراحةً في بند `COMPRESSION`. الأنواع المدعومة هي: `'none'`، `'gzip'`، `'deflate'`، `'br'`، `'xz'`، `'zstd'`، `'lz4'`، `'bz2'`.

هذه الميزة متاحة في [عميل سطر الأوامر](../../interfaces/client.md) و[clickhouse-local](../../operations/utilities/clickhouse-local.md).

**أمثلة**

<div id="single-file-with-from-infile">
  ### ملف واحد مع FROM INFILE
</div>

نفّذ الاستعلامات التالية باستخدام [عميل سطر الأوامر](../../interfaces/client.md):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### ملفات متعددة مع FROM INFILE باستخدام أنماط glob
</div>

هذا المثال مشابه جدًا للمثال السابق، لكن عمليات الإدراج تُنفَّذ من عدة ملفات باستخدام `FROM INFILE 'input_*.csv`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
بالإضافة إلى اختيار عدة ملفات باستخدام `*`، يمكنك استخدام النطاقات (`{1,2}` أو `{1..9}`) وغيرها من [استبدالات glob](/ar/sql-reference/table-functions/file.md/#globs-in-path). ستعمل هذه الأمثلة الثلاثة جميعًا مع المثال أعلاه:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## الإدراج باستخدام دالة جدولية
</div>

يمكن إدراج البيانات في الجداول المُشار إليها باستخدام [دوال الجدول](../../sql-reference/table-functions/index.md).

**الصيغة**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**مثال**

تُستخدم الدالة الجدولية [remote](/ar/sql-reference/table-functions/remote) في الاستعلامات التالية:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## الإدراج في ClickHouse Cloud
</div>

توفّر الخدمات على ClickHouse Cloud، افتراضيًا، عدة نُسخ متماثلة لضمان التوافر العالي. وعند الاتصال بخدمة، يُنشأ اتصال بإحدى هذه النُسخ المتماثلة.

بعد نجاح عملية `INSERT`، تُكتب البيانات إلى طبقة التخزين الأساسية. ومع ذلك، قد يستغرق وصول هذه التحديثات إلى النُسخ المتماثلة بعض الوقت. لذلك، إذا استخدمت اتصالًا آخر ينفّذ استعلام `SELECT` على إحدى تلك النُسخ المتماثلة الأخرى، فقد لا تظهر البيانات المحدَّثة بعد.

يمكن استخدام `select_sequential_consistency` لإجبار النسخة المتماثلة على تلقّي أحدث التحديثات. وفيما يلي مثال على استعلام `SELECT` يستخدم هذا الإعداد:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

لاحظ أن استخدام `select_sequential_consistency` سيزيد العبء على ClickHouse Keeper (الذي يستخدمه ClickHouse Cloud داخليًا)، وقد يؤدي إلى تباطؤ الأداء بحسب الحمل الواقع على الخدمة. نوصي بعدم تمكين هذا الإعداد إلا عند الضرورة. والنهج الموصى به هو تنفيذ عمليات القراءة/الكتابة ضمن الجلسة نفسها، أو استخدام برنامج تشغيل عميل يعتمد البروتوكول الأصلي (وبالتالي يدعم الاتصالات المثبتة).

<div id="inserting-into-a-replicated-setup">
  ## الإدراج في إعداد مُكرَّر
</div>

في إعداد مُكرَّر، تصبح البيانات مرئية على النسخ المتماثلة الأخرى بعد اكتمال تكرارها. وتبدأ عملية تكرار البيانات (أي تنزيلها إلى النسخ المتماثلة الأخرى) مباشرةً بعد تنفيذ `INSERT`. ويختلف ذلك عن ClickHouse Cloud، حيث تُكتَب البيانات فورًا إلى التخزين المشترك، وتتابع النسخ المتماثلة تغييرات البيانات الوصفية.

لاحظ أنه في الإعدادات المُكرَّرة، قد تستغرق عمليات `INSERT` أحيانًا وقتًا ملحوظًا (في حدود ثانية واحدة)، لأنها تتطلب إجراء commit إلى ClickHouse Keeper لتحقيق التوافق الموزّع. كما أن استخدام S3 للتخزين يضيف زمن انتقال إضافيًا.

<div id="performance-considerations">
  ## اعتبارات الأداء
</div>

يقوم `INSERT` بفرز بيانات الإدخال حسب المفتاح الأساسي وتقسيمها إلى أقسام باستخدام مفتاح التقسيم. وإذا أدرجت بيانات في عدة أقسام دفعةً واحدة، فقد ينخفض أداء استعلام `INSERT` بشكل كبير. لتجنّب ذلك:

* أدرج البيانات في دفعات كبيرة نسبيًا، مثل 100,000 صف في كل مرة.
* اجمع البيانات حسب مفتاح التقسيم قبل تحميلها إلى ClickHouse.

لن يتراجع الأداء إذا:

* أُدرجت البيانات في الوقت الفعلي.
* حمّلت بيانات تكون عادةً مرتبة حسب الوقت.

<div id="asynchronous-inserts">
  ### عمليات الإدراج غير المتزامنة
</div>

يمكن إدراج البيانات بصورة غير متزامنة من خلال عمليات إدراج صغيرة لكنها متكررة. تُجمَع بيانات عمليات الإدراج هذه في دفعات، ثم تُدرَج بأمان في جدول. لاستخدام عمليات الإدراج غير المتزامنة، فعِّل الإعداد [`async_insert`](/ar/operations/settings/settings#async_insert).

يؤدي استخدام `async_insert` أو [محرك الجدول `Buffer`](/ar/engines/table-engines/special/buffer) إلى تخزين مؤقت إضافي.

<div id="large-or-long-running-inserts">
  ### عمليات الإدراج الكبيرة أو طويلة التشغيل
</div>

عند إدراج كميات كبيرة من البيانات، يعمل ClickHouse على تحسين أداء الكتابة من خلال عملية تُسمّى &quot;squashing&quot;. إذ تُدمَج كتل صغيرة من البيانات المُدرجة في الذاكرة وتُجمَّع في كتل أكبر قبل كتابتها إلى القرص. ويقلّل squashing من التكلفة الإضافية المرتبطة بكل عملية كتابة. وخلال هذه العملية، تصبح البيانات المُدرجة متاحة للاستعلام بعد أن يُكمل ClickHouse كتابة كل [`max_insert_block_size`](/ar/operations/settings/settings#max_insert_block_size) صف.

**انظر أيضًا**

* [async&#95;insert](/ar/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/ar/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/ar/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/ar/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/ar/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/ar/operations/settings/settings#async_insert_max_data_size)