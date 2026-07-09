---
description: 'توثيق الجدول'
keywords: ['الضغط', 'خوارزمية الضغط', 'مخطط', 'DDL']
sidebar_label: 'جدول'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'مرجع'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ينشئ جدولًا جديدًا. ويمكن أن يتخذ هذا الاستعلام صيغ بناء الجملة متعددة بحسب حالة الاستخدام.

افتراضيًا، لا تُنشأ الجداول إلا على الخادم الحالي. وتُنفَّذ استعلامات DDL الموزعة باستخدام العبارة `ON CLUSTER`، وهي [موصوفة بشكل منفصل](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## صيغ بناء الجملة
</div>

<div id="with-explicit-schema">
  ### مع مخطط محدد صراحةً
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

ينشئ جدولًا باسم `table_name` في قاعدة البيانات `db`، أو في قاعدة البيانات الحالية إذا لم يتم تعيين `db`، بالبنية المحددة بين الأقواس وباستخدام المحرك `engine`.
تتألف بنية الجدول من قائمة بأوصاف الأعمدة، والفهارس الثانوية، والإسقاطات، والقيود. وإذا كان [المفتاح الأساسي](#primary-key) مدعومًا من المحرك، فسيُذكر كمعامل لمحرك الجدول.

في أبسط الحالات، يكون وصف العمود بالشكل `name type`. مثال: `RegionID UInt32`.

يمكن أيضًا تعريف تعبيرات للقيم الافتراضية (انظر أدناه).

عند الحاجة، يمكن تحديد المفتاح الأساسي باستخدام تعبير مفتاح واحد أو أكثر.

يمكن إضافة تعليقات للأعمدة وللجدول.

<div id="with-a-schema-similar-to-other-table">
  ### مع مخطط جدول موجود
</div>

يدعم ClickHouse نسخ مخطط جدول موجود وبياناته.

لنسخ مخطط جدول موجود:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

يؤدي هذا إلى إنشاء جدول له نفس بنية جدول آخر.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### مع مخطط جدول موجود وبياناته
</div>

لاستنساخ مخطط جدول موجود وبياناته:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

يؤدي ذلك إلى إنشاء جدول له نفس البنية والبيانات الموجودة في جدول حالي. بعد إنشاء الجدول الجديد، تُلحَق به جميع الأقسام من `db.table`. وبعبارة أخرى، تُستنسخ بيانات `db.table` إلى `db2.table_clone` عند إنشائه. هذا الاستعلام مكافئ لما يلي:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

بالنسبة إلى كلتا الميزتين، يمكنك تحديد محرك مختلف للجدول. وإذا لم يُحدَّد المحرك، فسيُستخدم المحرك نفسه المستخدم للجدول الأصلي (`db.table`).

<div id="from-a-table-function">
  ### باستخدام دالة جدول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

ينشئ جدولًا يعطي النتيجة نفسها التي تعطيها [دالة الجدول](/ar/sql-reference/table-functions) المحددة. وسيعمل الجدول الذي أُنشئ أيضًا بالطريقة نفسها التي تعمل بها دالة الجدول المقابلة المحددة.

<div id="from-select-query">
  ### من خلال استعلام SELECT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

ينشئ جدولًا ببنية مشابهة لنتيجة استعلام `SELECT`، باستخدام المحرك `engine`، ويملؤه بالبيانات من `SELECT`. ويمكنك أيضًا تحديد وصف الأعمدة صراحةً.

إذا كان الجدول موجودًا بالفعل وتم تحديد `IF NOT EXISTS`، فلن ينفّذ الاستعلام أي شيء.

قد توجد بنود أخرى بعد بند `ENGINE` في الاستعلام. راجع الوثائق التفصيلية حول كيفية إنشاء الجداول في أوصاف [محركات الجداول](/ar/engines/table-engines).

**مثال**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## مُعدِّلات `NULL` و`NOT NULL`
</div>

تسمح مُعدِّلات `NULL` و`NOT NULL` التي تأتي بعد نوع البيانات في تعريف العمود بأن يكون [Nullable](/ar/sql-reference/data-types/nullable) أو تمنع ذلك.

إذا لم يكن النوع `Nullable` وتم تحديد `NULL`، فسيُعامَل على أنه `Nullable`؛ أما إذا تم تحديد `NOT NULL`، فلن يُعامَل كذلك. على سبيل المثال، `INT NULL` يعادل `Nullable(INT)`. وإذا كان النوع `Nullable` وتم تحديد المُعدِّلَين `NULL` أو `NOT NULL`، فسيتم طرح استثناء.

راجع أيضًا الإعداد [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable).

<div id="default_values">
  ## القيم الافتراضية
</div>

يمكن أن يحدِّد وصف العمود تعبيرًا للقيمة الافتراضية على الشكل `DEFAULT expr` أو `MATERIALIZED expr` أو `ALIAS expr`. مثال: `URLDomain String DEFAULT domain(URL)`.

يكون التعبير `expr` اختياريًا. وإذا تم حذفه، فيجب تحديد نوع العمود صراحةً، وتكون القيمة الافتراضية `0` للأعمدة الرقمية، و`''` (السلسلة الفارغة) للأعمدة النصية، و`[]` (المصفوفة الفارغة) لأعمدة المصفوفات، و`1970-01-01` لأعمدة التاريخ، أو `NULL` للأعمدة من النوع Nullable.

يمكن حذف نوع العمود في عمود القيمة الافتراضية، وفي هذه الحالة يُستدل عليه من نوع `expr`. على سبيل المثال، سيكون نوع العمود `EventDate DEFAULT toDate(EventTime)` هو Date.

إذا تم تحديد كلٍّ من نوع بيانات وتعبير قيمة افتراضية، فستُدرج ضمنيًا دالة لتحويل النوع تقوم بتحويل التعبير إلى النوع المحدد. مثال: يُمثَّل `Hits UInt32 DEFAULT 0` داخليًا على أنه `Hits UInt32 DEFAULT toUInt32(0)`.

قد يشير تعبير القيمة الافتراضية `expr` إلى أي أعمدة في الجدول وإلى ثوابت. ويتحقق ClickHouse من أن التغييرات في بنية الجدول لا تؤدي إلى إدخال حلقات في حساب التعبير. وبالنسبة إلى INSERT، فإنه يتحقق من أن التعبيرات قابلة للحل، أي إن جميع الأعمدة التي يمكن حسابها انطلاقًا منها قد تم تمريرها.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

القيمة الافتراضية العادية. إذا لم تُحدَّد قيمة هذا العمود في استعلام `INSERT`، فستُحتسب من `expr`.

مثال:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

تعبير MATERIALIZED. تُحتسَب قيم هذه الأعمدة تلقائيًا وفقًا لتعبير MATERIALIZED المحدَّد عند إدراج الصفوف. ولا يمكن تحديد القيم فيها صراحةً أثناء عمليات `INSERT`.

كذلك، لا تُضمَّن أعمدة القيم الافتراضية من هذا النوع في نتيجة `SELECT *`. وذلك للحفاظ على الخاصية الثابتة التي تضمن أن نتيجة `SELECT *` يمكن دائمًا إدراجها مرة أخرى في الجدول باستخدام `INSERT`. ويمكن تعطيل هذا السلوك باستخدام الإعداد `asterisk_include_materialized_columns`.

مثال:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

عمود سريع الزوال. لا تُخزَّن الأعمدة من هذا النوع في الجدول، ولا يمكن إجراء SELECT عليها. والغرض الوحيد من الأعمدة سريعة الزوال هو إنشاء تعبيرات القيمة الافتراضية لأعمدة أخرى انطلاقًا منها.

أي عملية insert بدون تحديد الأعمدة صراحةً ستتخطّى الأعمدة من هذا النوع. وذلك للحفاظ على الخاصية الثابتة التي مفادها أن نتيجة `SELECT *` يمكن دائمًا إدراجها مرة أخرى في الجدول باستخدام `INSERT`.

مثال:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

الأعمدة المحسوبة (مرادف). لا تُخزَّن الأعمدة من هذا النوع في الجدول، ولا يمكن INSERT قيم فيها.

عندما تشير استعلامات SELECT صراحةً إلى أعمدة من هذا النوع، تُحتسب القيمة وقت الاستعلام من `expr`. وبشكل افتراضي، يستبعد `SELECT *` أعمدة ALIAS. ويمكن تعطيل هذا السلوك باستخدام الإعداد `asterisk_include_alias_columns`.

عند استخدام استعلام ALTER لإضافة أعمدة جديدة، لا تُكتب البيانات القديمة لهذه الأعمدة. وبدلًا من ذلك، عند قراءة البيانات القديمة التي لا تحتوي على قيم للأعمدة الجديدة، تُحتسب التعبيرات آنيًا بشكل افتراضي. ومع ذلك، إذا كان احتساب هذه التعبيرات يتطلب أعمدة أخرى غير مذكورة في الاستعلام، فستُقرأ هذه الأعمدة أيضًا، ولكن فقط لكتل البيانات التي تحتاج إلى ذلك.

إذا أضفت عمودًا جديدًا إلى جدول ثم غيّرت لاحقًا تعبيره الافتراضي، فستتغير القيم المستخدمة للبيانات القديمة (أي البيانات التي لم تُخزَّن قيمها على القرص). لاحظ أنه عند تشغيل عمليات الدمج في الخلفية، تُكتب بيانات الأعمدة المفقودة في أحد الأجزاء الجاري دمجها إلى الجزء المدمج.

لا يمكن تعيين قيم افتراضية لعناصر بُنى البيانات المتداخلة.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## المفتاح الأساسي
</div>

يمكنك تعريف [مفتاح أساسي](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries) عند إنشاء جدول. ويمكن تحديد المفتاح الأساسي بإحدى طريقتين:

* ضمن قائمة الأعمدة

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* خارج قائمة الأعمدة

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
لا يمكنك الجمع بين الطريقتين ضمن استعلام واحد.
:::

<div id="constraints">
  ## القيود
</div>

يمكن أيضًا تعريف القيود إلى جانب أوصاف الأعمدة:

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

يمكن أن يكون `boolean_expr_1` أي تعبير منطقي. إذا كانت القيود معرّفة للجدول، فسيُتحقَّق من كلٍّ منها لكل صف في استعلام `INSERT`. وإذا لم يُستوفَ أي قيد، فسيردّ الخادم باستثناء يتضمّن اسم القيد وتعبير التحقق.

قد تؤثر إضافة عدد كبير من القيود سلبًا في أداء استعلامات `INSERT` الكبيرة.

يمكن الاطلاع على القيود الموجودة في جميع الجداول عبر جدول [`system.constraints`](/ar/operations/system-tables/constraints).

<div id="assume">
  ### ASSUME
</div>

يُستخدم البند `ASSUME` لتعريف `CONSTRAINT` على جدول يُفترض أنه صحيح. ويمكن للمُحسِّن بعد ذلك استخدام هذا القيد لتحسين أداء استعلامات SQL.

خذ هذا المثال حيث يُستخدم `ASSUME CONSTRAINT` عند إنشاء الجدول `users_a`:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

هنا، يُستخدم `ASSUME CONSTRAINT` للإشارة إلى أن الدالة `length(name)` تساوي دائمًا قيمة العمود `name_len`. وهذا يعني أنه كلما استُدعيت `length(name)` في استعلام، يمكن لـ ClickHouse استبدالها بـ `name_len`، وهو ما يُفترض أن يكون أسرع لأنه يتجنب استدعاء الدالة `length()`.

بعد ذلك، عند تنفيذ الاستعلام `SELECT name FROM users_a WHERE length(name) < 5;`، يمكن لـ ClickHouse تحسينه إلى `SELECT name FROM users_a WHERE name_len < 5`; بفضل `ASSUME CONSTRAINT`. ويمكن أن يؤدي ذلك إلى تنفيذ الاستعلام بشكل أسرع لأنه يتجنب حساب طول `name` لكل صف.

إن `ASSUME CONSTRAINT` **لا يفرض القيد**، بل يقتصر على إبلاغ المُحسِّن بأن القيد متحقق. وإذا لم يكن القيد صحيحًا بالفعل، فقد تكون نتائج الاستعلامات غير صحيحة. لذلك، يجب ألا تستخدم `ASSUME CONSTRAINT` إلا إذا كنت متأكدًا من صحة القيد.

<div id="ttl-expression">
  ## تعبير TTL
</div>

يحدّد مدة الاحتفاظ بالقيم. ولا يمكن تحديده إلا للجداول من عائلة MergeTree. للاطلاع على وصف مفصل، راجع [TTL للأعمدة والجداول](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

<div id="column_compression_codec">
  ## ترميزات الضغط للأعمدة
</div>

بشكل افتراضي، يطبّق ClickHouse ضغط `lz4` في النسخة ذاتية الإدارة، و`zstd` في ClickHouse Cloud.

بالنسبة إلى عائلة محركات `MergeTree`، يمكنك تغيير طريقة الضغط الافتراضية في قسم [الضغط](/ar/operations/server-configuration-parameters/settings#compression) ضمن تهيئة الخادم.

يمكنك أيضًا تحديد طريقة الضغط لكل عمود على حدة في استعلام `CREATE TABLE`.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

يمكن تحديد ترميز الضغط `Default` للإشارة إلى الضغط الافتراضي، الذي قد يعتمد أثناء وقت التشغيل على إعدادات مختلفة (وعلى خصائص البيانات).
مثال: `value UInt64 CODEC(Default)` — وهو مماثل لعدم تحديد أي ترميز ضغط.

يمكنك أيضًا إزالة `CODEC` الحالي من العمود واستخدام الضغط الافتراضي من config.xml:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

يمكن دمج ترميزات الضغط في خط أنابيب، على سبيل المثال: `CODEC(Delta, Default)`.

:::tip
لا يمكنك فك ضغط ملفات قاعدة بيانات ClickHouse باستخدام أدوات خارجية مثل `lz4`. استخدم بدلًا من ذلك الأداة الخاصة [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).
:::

يُدعَم الضغط في محركات الجداول التالية:

* عائلة [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md). تدعم ترميزات الضغط للأعمدة وإمكانية اختيار طريقة الضغط الافتراضية عبر إعدادات [الضغط](/ar/operations/server-configuration-parameters/settings#compression).
* عائلة [Log](../../../engines/table-engines/log-family/index.md). تستخدم طريقة الضغط `lz4` افتراضيًا، وتدعم ترميزات الضغط للأعمدة.
* [Set](../../../engines/table-engines/special/set.md). لا يدعم سوى الضغط الافتراضي.
* [Join](../../../engines/table-engines/special/join.md). لا يدعم سوى الضغط الافتراضي.

يدعم ClickHouse ترميزات الضغط العامة وترميزات الضغط المتخصصة.

<div id="general-purpose-codecs">
  ### ترميزات الضغط العامة
</div>

<div id="none">
  #### NONE
</div>

`NONE` — من دون ضغط.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — [خوارزمية ضغط بيانات](https://github.com/lz4/lz4) غير فقديّة تُستخدم افتراضيًا. تطبّق ضغط LZ4 السريع.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — خوارزمية LZ4 HC (ضغط عالٍ) ذات مستوى قابل للضبط. المستوى الافتراضي: 9. يؤدّي ضبط `level <= 0` إلى تطبيق المستوى الافتراضي. المستويات الممكنة: [1, 12]. نطاق المستويات الموصى به: [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [خوارزمية ضغط ZSTD](https://en.wikipedia.org/wiki/Zstandard) مع `level` قابل للضبط. المستويات الممكنة: [1, 22]. المستوى الافتراضي: 1.

تكون مستويات الضغط العالية مفيدة في السيناريوهات غير المتماثلة، مثل الضغط مرة واحدة وفك الضغط مرارًا. وتعني المستويات الأعلى ضغطًا أفضل وزيادة في استخدام CPU.

<div id="zstd_qat">
  #### متقادم: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### متقادمة: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### ترميزات الضغط المتخصصة
</div>

صُمِّمت ترميزات الضغط هذه لجعل الضغط أكثر فعالية من خلال الاستفادة من خصائص محددة في البيانات. بعض هذه ترميزات الضغط لا يضغط البيانات بنفسه، بل يُجري عليها معالجة مسبقة بحيث تتمكن مرحلة ضغط ثانية تستخدم ترميز ضغط للأغراض العامة من تحقيق معدل ضغط أعلى للبيانات.

<div id="delta">
  #### دلتا
</div>

`Delta(delta_bytes)` — أسلوب ضغط تُستبدل فيه القيم الخام بالفارق بين كل قيمتين متجاورتين، باستثناء القيمة الأولى التي تبقى دون تغيير. يمثّل `delta_bytes` الحجم الأقصى للقيم الخام، والقيمة الافتراضية هي `sizeof(type)`. يُعد تمرير `delta_bytes` كوسيطة أمرًا متقادمًا، وسيُزال دعمه في إصدار مستقبلي. دلتا هو codec لتحضير البيانات، أي لا يمكن استخدامه بصورة مستقلة.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — يحسب دلتا الفروق ويكتبها بصيغة ثنائية مدمجة. يحمل `bytes_size` معنى مشابهًا لـ `delta_bytes` في ترميز [Delta](#delta). لم يعد تحديد `bytes_size` كوسيط مُوصى به، وستُزال دعمه في إصدار مستقبلي. تتحقق معدلات الضغط المثلى للتسلسلات الرتيبة ذات `stride` الثابت، مثل بيانات السلاسل الزمنية. ويمكن استخدامه مع أي نوع رقمي. يطبّق الخوارزمية المستخدمة في Gorilla TSDB، مع توسيعها لدعم الأنواع ذات 64 بت. ويستخدم بتًا إضافيًا واحدًا لدلتا 32 بت: بادئات من 5 بت بدلًا من بادئات من 4 بت. لمزيد من المعلومات، راجع Compressing Time Stamps في [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). يُعد DoubleDelta ترميزًا لإعداد البيانات، أي لا يمكن استخدامه بشكل مستقل.

<div id="gcd">
  #### GCD
</div>

`GCD()` - - يحسب القاسم المشترك الأكبر (GCD) للقيم في العمود، ثم يقسم كل قيمة على هذا القاسم. يمكن استخدامه مع الأعمدة الصحيحة والعشرية وأعمدة التاريخ/الوقت. يناسب هذا الـ codec الأعمدة التي تتغير قيمها (بالزيادة أو النقصان) بمضاعفات GCD، مثل 24 و28 و16 و24 و8 و24 ‏(GCD = 4). يُعد GCD codec لإعداد البيانات، أي لا يمكن استخدامه بشكل مستقل.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — يحسب XOR بين قيمة الفاصلة العائمة الحالية والسابقة، ويكتب الناتج في صيغة ثنائية مدمجة. كلما كان الفرق بين القيم المتتالية أصغر، أي كلما كان تغيّر قيم السلسلة أبطأ، كان معدل الضغط أفضل. ينفّذ الخوارزمية المستخدمة في Gorilla TSDB، مع توسيعها لدعم الأنواع ذات 64 بت. قيم `bytes_size` الممكنة هي: 1 و2 و4 و8، وتكون القيمة الافتراضية هي `sizeof(type)` إذا كانت مساويةً لـ 1 أو 2 أو 4 أو 8. وفي جميع الحالات الأخرى، تكون 1. لمزيد من المعلومات، راجع القسم 4.1 في [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — ضغط تكيفي عديم الفقد للبيانات ذات الفاصلة العائمة. يدعم `Float32` و`Float64`. لمزيد من التفاصيل، راجع [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

يقبل هذا الـ codec وسيط `variant` اختياريًا:

* `ALP()` أو `ALP(AUTO)` (الافتراضي) — يستخدم STD، ويعود إلى RD استنادًا إلى الحجم المضغوط المقدَّر.
* `ALP(STD)` — متغيّر ALP القياسي. يمثّل كل قيمة كعدد صحيح مضبوط بدقة باستخدام قوى عشرة، ثم يضغط الأعداد الصحيحة الناتجة باستخدام Frame-of-Reference وbit-packing. وتُخزَّن القيم غير القابلة للتمثيل كاستثناءات خام. يعمل بأفضل شكل مع الأرقام المشتقة من القيم العشرية (مثل القياسات والأسعار).
* `ALP(RD)` — متغيّر Real Doubles. يعيد تفسير نمط البتات لكل قيمة ويقسّمه إلى جزء علوي (الإشارة + الأس + البتات العليا من المانتيسا) وجزء سفلي. وتُرمَّز الأجزاء العلوية باستخدام قاموس (حتى 8 إدخالات)، بينما تُحزَّم الأجزاء السفلية على مستوى البتات. يعمل بأفضل شكل عندما تشترك قيم كثيرة في البتات العليا نفسها.

:::note
هذا الـ codec تجريبي ويتطلب `SET allow_experimental_codecs = 1` لاستخدامه.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - يتنبأ بشكل متكرر بقيمة floating-point التالية في التسلسل باستخدام الأفضل من بين متنبئين، ثم يُجري عملية XOR بين القيمة الفعلية والقيمة المتنبأ بها، ويضغط النتيجة باستخدام ضغط الأصفار البادئة. وهو مشابه لـ Gorilla، ويكون فعالًا عند تخزين series من قيم floating-point التي تتغير ببطء. بالنسبة إلى القيم ذات 64 بت (double)، يكون FPC أسرع من Gorilla، أما بالنسبة إلى القيم ذات 32 بت فقد يختلف الأداء. قيم `level` الممكنة هي: 1-28، والقيمة الافتراضية هي 12.  قيم `float_size` الممكنة هي: 4، 8، والقيمة الافتراضية هي `sizeof(type)` إذا كان النوع Float. وفي جميع الحالات الأخرى، تكون 4. للحصول على وصف مفصل للخوارزمية، راجع [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

<div id="t64">
  #### T64
</div>

`T64` — أسلوب ضغط يقتطع البتات العليا غير المستخدمة من القيم في أنواع البيانات الصحيحة (بما في ذلك `Enum` و`Date` و`DateTime`). في كل خطوة من خوارزميته، يأخذ الـ codec كتلة من 64 قيمة، ويضعها في مصفوفة بتات 64x64، ثم ينقلها، ويقتطع البتات غير المستخدمة من القيم، ويُرجع الباقي على هيئة تسلسل. والبتات غير المستخدمة هي البتات التي لا تختلف بين القيمتين العظمى والصغرى في data part بالكامل الذي يُستخدم له هذا الضغط.

يُستخدم codecا `DoubleDelta` و`Gorilla` في Gorilla TSDB بوصفهما مكوّنين من خوارزمية الضغط الخاصة به. ويكون أسلوب Gorilla فعّالًا في الحالات التي توجد فيها سلسلة من القيم المتغيرة ببطء مع طوابعها الزمنية. وتُضغط timestamps بكفاءة بواسطة codec `DoubleDelta`، كما تُضغط القيم بكفاءة بواسطة codec `Gorilla`. على سبيل المثال، للحصول على table مخزّن بكفاءة، يمكنك إنشاؤه بالتهيئة التالية:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### ترميزات ضغط التشفير
</div>

هذه ترميزات الضغط لا تضغط البيانات فعليًا، بل تُشفِّر البيانات على القرص. ولا تكون متاحة إلا عند تحديد مفتاح تشفير عبر إعدادات [encryption](/ar/operations/server-configuration-parameters/settings#encryption). لاحظ أن التشفير لا يكون ذا جدوى إلا في نهاية سلاسل ترميز الضغط، لأن البيانات المشفَّرة لا يمكن عادةً ضغطها بأي طريقة مفيدة.

ترميزات ضغط التشفير:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — يشفّر البيانات باستخدام AES-128 في وضع GCM-SIV وفقًا لـ [RFC 8452](https://tools.ietf.org/html/rfc8452).

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — يشفّر البيانات باستخدام AES-256 في وضع GCM-SIV.

تستخدم ترميزات الضغط هذه قيمة nonce ثابتة، لذا يكون التشفير حتميًا. وهذا يجعلها متوافقة مع المحركات التي تدعم إزالة التكرار مثل [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md)، ولكن لها نقطة ضعف: فعندما تُشفَّر كتلة البيانات نفسها مرتين، يكون النص المشفّر الناتج متماثلًا تمامًا، لذلك يمكن لمهاجم يستطيع قراءة القرص ملاحظة هذا التطابق (وإن كان التطابق فقط، من دون معرفة المحتوى).

:::note
تنشئ معظم المحركات، بما في ذلك عائلة &quot;*MergeTree&quot;، ملفات الفهرسة على القرص من دون تطبيق ترميزات الضغط. وهذا يعني أن النص الصريح سيظهر على القرص إذا كان العمود المشفّر مفهرسًا.
:::

:::note
إذا نفّذت استعلام SELECT يتضمن قيمة محددة في عمود مشفّر (مثلًا في جملة WHERE الخاصة به)، فقد تظهر هذه القيمة في [system.query&#95;log](../../../operations/system-tables/query_log.md). وقد ترغب في تعطيل التسجيل.
:::

**مثال**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
إذا كانت هناك حاجة إلى تطبيق الضغط، فيجب تحديده صراحةً. وإلا فلن يُطبَّق على البيانات إلا التشفير.
:::

**مثال**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## الجداول المؤقتة
</div>

:::note
يرجى ملاحظة أن الجداول المؤقتة لا تُكرَّر عبر النسخ المتماثلة. لذلك، لا يوجد ما يضمن أن تكون البيانات المُدرجة في جدول مؤقت متاحة في النسخ المتماثلة الأخرى. وتُعد الجداول المؤقتة مفيدة أساسًا عند الاستعلام عن مجموعات بيانات خارجية صغيرة أو تنفيذ join عليها ضمن جلسة واحدة.
:::

يدعم ClickHouse الجداول المؤقتة، والتي تتميز بالخصائص التالية:

* تختفي الجداول المؤقتة عند انتهاء الجلسة، بما في ذلك عند فقدان الاتصال.
* يستخدم الجدول المؤقت محرك الجدول Memory إذا لم يتم تحديد engine، ويمكنه استخدام أي محرك جدول باستثناء Replicated و`KeeperMap`.
* لا يمكن تحديد DB لجدول مؤقت، إذ يُنشأ خارج databases.
* يستحيل إنشاء جدول مؤقت باستخدام distributed DDL query على جميع خوادم cluster (باستخدام `ON CLUSTER`)، لأن هذا الجدول لا يوجد إلا ضمن الجلسة الحالية.
* إذا كان للجدول المؤقت الاسم نفسه لجدول آخر، وكان query يحدد اسم table من دون تحديد DB، فسيُستخدم الجدول المؤقت.
* في distributed query processing، تُمرَّر الجداول المؤقتة ذات محرك Memory المستخدمة في query إلى الخوادم البعيدة.

لإنشاء جدول مؤقت، استخدم الصياغة التالية:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

في معظم الحالات، لا تُنشأ الجداول المؤقتة يدويًا، بل عند استخدام بيانات خارجية في استعلام أو مع `(GLOBAL) IN`. لمزيد من المعلومات، راجع الأقسام ذات الصلة

يمكن استخدام الجداول ذات [ENGINE = Memory](../../../engines/table-engines/special/memory.md) بدلًا من الجداول المؤقتة.

<div id="replace-table">
  ## REPLACE TABLE
</div>

تتيح لك عبارة `REPLACE` تحديث جدول [بصورة ذرّية](/ar/concepts/glossary#atomicity).

:::note
تدعم هذه العبارة محركَي قاعدة البيانات [`Atomic`](../../../engines/database-engines/atomic.md) و[`Replicated`](../../../engines/database-engines/replicated.md)،
وهما محركا قاعدة البيانات الافتراضيان في ClickHouse وClickHouse Cloud على الترتيب.
:::

عادةً، إذا احتجت إلى حذف بعض البيانات من جدول،
فيمكنك إنشاء جدول جديد وملأه بعبارة `SELECT` لا تجلب البيانات غير المرغوب فيها،
ثم حذف الجدول القديم وإعادة تسمية الجدول الجديد.
يوضّح المثال أدناه هذا الأسلوب:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

بدلًا من الأسلوب المذكور أعلاه، يمكن أيضًا استخدام `REPLACE` (إذا كنت تستخدم محركات قواعد البيانات الافتراضية) لتحقيق النتيجة نفسها:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### بنية الجملة
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
تعمل جميع صيغ بناء الجملة الخاصة بتعليمة `CREATE` أيضًا مع هذه التعليمة. وسيؤدي استدعاء `REPLACE` لجدول غير موجود إلى حدوث خطأ.
:::

<div id="examples">
  ### أمثلة:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="محلي" default>
    لنأخذ الجدول التالي:

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    يمكننا استخدام عبارة `REPLACE` لمسح جميع البيانات:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    أو يمكننا استخدام عبارة `REPLACE` لتغيير بنية الجدول:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    لنأخذ الجدول التالي على ClickHouse Cloud:

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    يمكننا استخدام عبارة `REPLACE` لمسح جميع البيانات:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    أو يمكننا استخدام عبارة `REPLACE` لتغيير بنية الجدول:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## بند COMMENT
</div>

يمكنك إضافة تعليق إلى الجدول عند إنشائه.

**الصيغة**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
يجب تحديد بند `COMMENT` **بعد** أي بنود خاصة بالتخزين، مثل `PARTITION BY` و`ORDER BY` و`SETTINGS` الخاصة بالتخزين.

بعد بند `COMMENT`، لن يُحلَّل إلا `SETTINGS` الخاصة بالاستعلام (مثل `max_threads` وما إلى ذلك)، وليس الإعدادات المتعلقة بالتخزين.

وهذا يعني أن الترتيب الصحيح للبنود هو:

* `ENGINE`
* بنود التخزين
* `COMMENT`
* إعدادات الاستعلام (إن وُجدت)
  :::

**مثال**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## محتوى ذو صلة
</div>

* مدونة: [تحسين ClickHouse باستخدام المخططات والمرمّزات](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* مدونة: [العمل مع بيانات السلاسل الزمنية والدوال في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)