---
description: 'محرك جدول يخزّن سلاسل زمنية، أي مجموعة من القيم المرتبطة بطوابع زمنية ووسوم (أو تسميات).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'محرك جدول TimeSeries'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # محرك الجدول TimeSeries
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

محرك جدول لتخزين السلاسل الزمنية، أي مجموعة من القيم المرتبطة بطوابع زمنية ووسوم (أو تسميات):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
هذه ميزة تجريبية قد تتغير مستقبلًا بطرق غير متوافقة مع الإصدارات السابقة.
فعِّل استخدام محرك الجدول TimeSeries
باستخدام الإعداد [allow&#95;experimental&#95;time&#95;series&#95;table](/ar/operations/settings/settings#allow_experimental_time_series_table).
نفِّذ الأمر `set allow_experimental_time_series_table = 1`.
:::

<div id="syntax">
  ## الصيغة
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
للكلمة المحجوزة `SAMPLES` اسم مستعار هو `DATA`، ويُحتفَظ به للحفاظ على التوافق مع الإصدارات السابقة.
:::

<div id="usage">
  ## الاستخدام
</div>

من الأسهل البدء مع ترك كل شيء على الإعدادات الافتراضية (يُسمح بإنشاء جدول `TimeSeries` من دون تحديد قائمة بالأعمدة):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

بعد ذلك، يمكن استخدام هذا الجدول مع البروتوكولات التالية (يجب تعيين منفذ في تهيئة الخادم):

* [الكتابة البعيدة لـ Prometheus](/ar/interfaces/prometheus#remote-write)
* [القراءة البعيدة لـ Prometheus](/ar/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### الأعمدة الخارجية
</div>

تُولَّد أعمدة جدول TimeSeries تلقائيًا. وهذه هي الأعمدة الخارجية؛ فهي لا تخزّن أي بيانات، وإنما توفّر واجهة لـ `SELECT`/`INSERT`. تُخزَّن البيانات الفعلية في [الجداول الهدف](#target-tables). وفيما يلي قائمة بالأعمدة الخارجية:

| الاسم           | النوع                                             | الوصف                                                                                                                                                                                                                  |
| --------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | اسم المقياس                                                                                                                                                                                                            |
| `tags`          | `Map(String, String)`                             | خريطة الوسوم (labels) للسلسلة الزمنية                                                                                                                                                                                  |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` by default | مصفوفة من أزواج (الطابع الزمني، القيمة) لسلسلة زمنية. ويمكن اشتقاق نوعَي عنصر الطابع الزمني والعنصر scalar في Tuple من تعريف `INNER COLUMNS` الخاص بالعينات (انظر [تحديد الأعمدة الخارجية](#specifying-outer-columns)) |
| `metric_family` | `String`                                          | اسم عائلة المقياس (للبيانات الوصفية للمقاييس)                                                                                                                                                                          |
| `type`          | `String`                                          | نوع المقياس (مثل &quot;counter&quot; و&quot;gauge&quot;)                                                                                                                                                               |
| `unit`          | `String`                                          | وحدة المقياس                                                                                                                                                                                                           |
| `help`          | `String`                                          | وصف المقياس                                                                                                                                                                                                            |

مثال:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

يُسمح بترك `metric_name` فارغًا عند الإدراج، وهذا يعني أن اسم المقياس يُحدَّد في `tags` تحت `__name__`، على سبيل المثال:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

لإدراج البيانات الوصفية للمقاييس، أدرِجها في الأعمدة `metric_family` و`type` و`unit` و`help`:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### تحديد الأعمدة الخارجية
</div>

يمكن إدراج العمود الخارجي `time_series` صراحةً في عبارة `CREATE TABLE` لتجاوز نوعه الافتراضي `Array(Tuple(DateTime64(3), Float64))`. يستخرج ClickHouse نوعَي الطابع الزمني والقيمة العددية المفردة من Tuple ويمرّرهما إلى جدول العينات الداخلي:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

وهذا يعادل تعريف نوعَي عمودَي الطابع الزمني والقيمة مباشرةً في عبارة `INNER COLUMNS` الخاصة بـ samples:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

إذا استُخدم الشكلان معًا في عبارة `CREATE TABLE` نفسها، فيجب أن تتطابق الأنواع المُعلنة.

<div id="target-tables">
  ## الجداول الهدف
</div>

لا يحتوي جدول `TimeSeries` على بيانات خاصة به، إذ يُخزَّن كل شيء في الجداول الهدف التابعة له.
وهذا يشبه طريقة عمل [العرض المادي](../../../sql-reference/statements/create/view#materialized-view)،
مع فارق أن العرض المادي له جدول هدف واحد،
بينما يحتوي جدول `TimeSeries` على ثلاثة جداول هدف تحمل أسماء [samples](#samples-table) و[tags](#tags-table) و[metrics](#metrics-table).

يمكن تحديد الجداول الهدف صراحةً في استعلام `CREATE TABLE`
أو يمكن لمحرك جدول `TimeSeries` إنشاء جداول هدف داخلية تلقائيًا.

تُحوَّل الصفوف المُدرجة في جدول `TimeSeries`، وتُقسَّم إلى كتل، ثم تُدرَج في هذه الجداول الهدف الثلاثة.

الجداول الهدف هي كما يلي:

<div id="samples-table">
  ### جدول العينات
</div>

يحتوي جدول *samples* على سلاسل زمنية مرتبطة بمعرّف معيّن.

يجب أن يحتوي جدول *samples* على الأعمدة التالية:

| الاسم       | إلزامي؟ | النوع الافتراضي | الأنواع الممكنة        | الوصف                                  |
| ----------- | ------- | --------------- | ---------------------- | -------------------------------------- |
| `id`        | [x]     | `UUID`          | أيّ                    | يحدّد مجموعة من أسماء المقاييس والوسوم |
| `timestamp` | [x]     | `DateTime64(3)` | `DateTime64(X)`        | نقطة زمنية                             |
| `value`     | [x]     | `Float64`       | `Float32` أو `Float64` | قيمة مرتبطة بـ `timestamp`             |

<div id="tags-table">
  ### جدول الوسوم
</div>

يحتوي جدول *tags* على المعرّفات المحسوبة لكل مجموعة من اسم مقياس والوسوم.

يجب أن يحتوي جدول *tags* على الأعمدة التالية:

| الاسم                | إلزامي؟ | النوع الافتراضي                       | الأنواع الممكنة                                                                                                         | الوصف                                                                                                                                                           |
| -------------------- | ------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | [x]     | `UUID`                                | أي نوع (يجب أن يطابق نوع `id` في جدول [العينات](#samples-table))                                                        | يعرّف `id` مجموعة من اسم مقياس والوسوم. ويحدّد تعبير DEFAULT كيفية حساب هذا المعرّف                                                                             |
| `metric_name`        | [x]     | `LowCardinality(String)`              | `String` أو `LowCardinality(String)`                                                                                    | اسم المقياس                                                                                                                                                     |
| `<tag_value_column>` | [ ]     | `String`                              | `String` أو `LowCardinality(String)` أو `LowCardinality(Nullable(String))`                                              | قيمة وسم معيّن، ويُحدَّد اسم الوسم واسم العمود المقابل له في إعداد [tags&#95;to&#95;columns](#settings)                                                         |
| `tags`               | [x]     | `Map(LowCardinality(String), String)` | `Map(String, String)` أو `Map(LowCardinality(String), String)` أو `Map(LowCardinality(String), LowCardinality(String))` | خريطة للوسوم باستثناء الوسم `__name__` الذي يحتوي على اسم المقياس، وباستثناء الوسوم التي تَرِد أسماؤها في إعداد [tags&#95;to&#95;columns](#settings)            |
| `all_tags`           | [ ]     | `Map(String, String)`                 | `Map(String, String)` أو `Map(LowCardinality(String), String)` أو `Map(LowCardinality(String), LowCardinality(String))` | عمود مؤقت، يكون كل صف فيه خريطةً لجميع الوسوم باستثناء الوسم `__name__` فقط الذي يحتوي على اسم المقياس. والغرض الوحيد من هذا العمود هو استخدامه أثناء حساب `id` |
| `min_time`           | [ ]     | `Nullable(DateTime64(3))`             | `DateTime64(X)` أو `Nullable(DateTime64(X))`                                                                            | الحد الأدنى للطابع الزمني للسلاسل الزمنية ذات `id` هذا. يُنشأ العمود إذا كانت قيمة [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) هي `true`       |
| `max_time`           | [ ]     | `Nullable(DateTime64(3))`             | `DateTime64(X)` أو `Nullable(DateTime64(X))`                                                                            | الحد الأقصى للطابع الزمني للسلاسل الزمنية ذات `id` هذا. يُنشأ العمود إذا كانت قيمة [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) هي `true`       |

<div id="metrics-table">
  ### جدول المقاييس
</div>

يحتوي جدول *metrics* على بعض المعلومات عن المقاييس التي جُمعت، وأنواعها، وأوصافها.

يجب أن يحتوي جدول *metrics* على الأعمدة التالية:

| الاسم                | إلزامي؟ | النوع الافتراضي          | الأنواع الممكنة                      | الوصف                                                                                                                                                                                        |
| -------------------- | ------- | ------------------------ | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x]     | `String`                 | `String` أو `LowCardinality(String)` | اسم عائلة المقاييس                                                                                                                                                                           |
| `type`               | [x]     | `LowCardinality(String)` | `String` أو `LowCardinality(String)` | نوع عائلة المقاييس، ويكون إحدى القيم التالية: &quot;counter&quot; أو &quot;gauge&quot; أو &quot;summary&quot; أو &quot;stateset&quot; أو &quot;histogram&quot; أو &quot;gaugehistogram&quot; |
| `unit`               | [x]     | `LowCardinality(String)` | `String` أو `LowCardinality(String)` | الوحدة المستخدمة في المقياس                                                                                                                                                                  |
| `help`               | [x]     | `String`                 | `String` أو `LowCardinality(String)` | وصف المقياس                                                                                                                                                                                  |

<div id="creation">
  ## الإنشاء
</div>

توجد عدة طرق لإنشاء جدول باستخدام محرك الجدول `TimeSeries`.
أبسط صيغة

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

سيؤدي هذا فعليًا إلى إنشاء الجدول التالي (يمكنك التحقق من ذلك بتنفيذ `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

لذا، جرى إنشاء الأعمدة تلقائيًا، وهناك أيضًا ثلاثة جداول هدف داخلية، لكلٍ منها تعريفات الأعمدة الخاصة بها
المخزنة في عبارات `INNER COLUMNS`.

تحمل جداول الهدف الداخلية أسماءً مثل `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
ولكل جدول هدف مجموعته الخاصة من الأعمدة:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## إنشاء جدول باستخدام AS من جدول موجود
</div>

تعليمة `CREATE TABLE new_table AS existing_table` تنسخ من `existing_table` ما يلي:

* `SETTINGS`
* `INNER COLUMNS` لكل نوع
* `INNER ENGINE` لكل نوع

لا يُسمح بهذه التعليمة إذا كان `existing_table` يحتوي على أهداف خارجية.
تُعاد توليد قائمة الأعمدة الخارجية ولا تُنسخ.

<div id="adjusting-column-types">
  ## ضبط أنواع الأعمدة
</div>

يمكنك تعديل أنواع الأعمدة في الجداول الهدف الداخلية باستخدام عبارة `INNER COLUMNS`. على سبيل المثال، لتخزين الطوابع الزمنية بالميكروثانية والقيم كـ `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

يمكن استخدام الجملة نفسها لتحديد ترميزات الضغط وسمات الأعمدة الأخرى:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## عمود `id`
</div>

يحتوي عمود `id` على معرّفات، ويُحسَب كل معرّف استنادًا إلى تركيبة من اسم المقياس والوسوم.
يمكن تخصيص النوع وتعبير `DEFAULT` المستخدَم لإنشاء المعرّفات عبر عبارة `TAGS INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

يجب أن يكون نوع العمود `id` أحد الأنواع التالية: `UUID` أو `UInt64` أو `UInt128` أو `FixedString(16)`. إذا لم يتم تحديد تعبير `DEFAULT`، فسيحدده ClickHouse تلقائيًا بناءً على نوع `id`. يجب أن تتطابق أنواع `id` المعلنة في الجدولين الداخليين samples وtags.

يوفّر الإعداد `id_generator` التخصيص نفسه من دون استخدام العبارة `INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

إذا كان هذا الإعداد معيّنًا، فسيُستخدم لإنشاء `id` حتى إذا كان `DEFAULT` الخاص بالعمود يحتوي على تعبير مختلف.

<div id="tags-and-all-tags">
  ## العمودان `tags` و`all_tags`
</div>

هناك عمودان يحتويان على خرائط للوسوم، هما `tags` و`all_tags`. في هذا المثال، يدلان على الشيء نفسه، لكن قد يختلفان
إذا استُخدم الإعداد `tags_to_columns`. يتيح هذا الإعداد تحديد وسم معيّن لتخزينه في عمود منفصل بدلًا من تخزينه
في خريطة داخل العمود `tags`:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

ستضيف هذه العبارة العمودين `instance` و`job` إلى الجدول الهدف الداخلي [tags](#tags-table).
في هذه الحالة، لن يحتوي العمود `tags` على الوسمين `instance` و`job`،
لكن العمود `all_tags` سيحتوي عليهما. ويُعد العمود `all_tags` مؤقتًا، وغرضه الوحيد هو استخدامه في تعبير DEFAULT
للعمود `id`.

<div id="inner-table-engines">
  ## محركات الجداول الخاصة بالجداول الهدف الداخلية
</div>

تستخدم الجداول الهدف الداخلية، افتراضيًا، محركات الجداول التالية:

* يستخدم جدول [samples](#samples-table) محرك [MergeTree](../mergetree-family/mergetree)؛
* يستخدم جدول [tags](#tags-table) محرك [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) لأن البيانات نفسها تُدرَج في هذا الجدول عدة مرات في كثير من الأحيان، لذا نحتاج إلى طريقة
  لإزالة التكرارات، وكذلك لأن إجراء التجميع مطلوب للعمودين `min_time` و `max_time`؛
* يستخدم جدول [metrics](#metrics-table) محرك [ReplacingMergeTree](../mergetree-family/replacingmergetree) لأن البيانات نفسها تُدرَج في هذا الجدول عدة مرات في كثير من الأحيان، لذا نحتاج إلى طريقة
  لإزالة التكرارات.

يمكن أيضًا استخدام محركات جداول أخرى للجداول الهدف الداخلية إذا تم تحديد ذلك على هذا النحو:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

يُبقي جدول [الوسوم](#tags-table) أعمدة الوسوم (وخرائط `tags`/`all_tags` من النوع Maps) خارج مفتاح الفرز الخاص به،
وهذا ما يرفضه `AggregatingMergeTree` افتراضيًا (راجع [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)).
وهذا آمن هنا لأن هذه الأعمدة تعتمد وظيفيًا على `id`، وهو جزء من مفتاح الفرز، لذا فإن جميع
الصفوف التي تدمجها عملية دمج في الخلفية معًا تكون لها القيم نفسها. وعندما يُنشأ جدول الوسوم الداخلي أو يُحدَّد
محركه بشكل مضمَّن كما هو موضح أعلاه، يضبط `TimeSeries` القيمة `allow_dimensions_outside_sorting_key = 1` عليه تلقائيًا؛
أما إذا كان جدول الوسوم التجميعي [الخارجي](#external-target-tables) مُنشأً يدويًا، فيجب عليك ضبطها بنفسك.

<div id="external-target-tables">
  ## جداول الهدف الخارجية
</div>

يمكن إعداد جدول `TimeSeries` لاستخدام جدول أُنشئ يدويًا:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

يجب أن تتطابق أنواع أعمدة الجداول الخارجية (`id` و`timestamp` و`value` وأعمدة `<tag_value_column>` المدرجة في [`tags_to_columns`](#settings)) مع الأنواع التي كان جدول `TimeSeries` سيولّدها داخليًا لولا ذلك (راجع [جدول العينات](#samples-table) و[جدول الوسوم](#tags-table) و[جدول المقاييس](#metrics-table) للاطلاع على قيود الأنواع). ويُبلَّغ عن عدم تطابق الأنواع عند وقت `CREATE`.

يُحدَّد تعبير مولِّد المعرّف للهدف الخارجي للوسوم عند وقت INSERT بالترتيب التالي: إعداد [`id_generator`](#settings) (إذا كان مضبوطًا)، ثم `DEFAULT` المُعلَن في عمود `id` للجدول الخارجي (إن وُجد)، ثم المولِّد القياسي المستمد من نوع `id`. ومن ثمّ، فإن هذا الإعداد يتجاوز أي `DEFAULT` مُعلَن في الجدول الخارجي — راجع [عمود `id`](#id-column) للتفاصيل.

<div id="altering-settings">
  ## تعديل الإعدادات
</div>

يمكن تغيير إعدادين بعد تنفيذ `CREATE`:

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

لاحظ أن تغيير `id_generator` بعد وجود بيانات بالفعل في جدول Tags قد يؤدي إلى إنشاء معرّفات مختلفة لنفس تركيبة `metric+tag` — إذ تحتفظ الصفوف القديمة بمعرّفاتها القديمة، بينما تستخدم الصفوف الجديدة المُولِّد الجديد.

أما الإعدادات الأخرى، فلا يمكن تغييرها باستخدام `ALTER ... MODIFY SETTING` لأنها تكون مدمجة في مخطط الجداول الداخلية وقت `CREATE`.

<div id="settings">
  ## الإعدادات
</div>

فيما يلي قائمة بالإعدادات التي يمكن تحديدها عند تعريف جدول `TimeSeries`:

| الاسم                                | النوع      | الافتراضي          | الوصف                                                                                                                                                                                                                       |
| ------------------------------------ | ---------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expression | يعتمد على نوع `id` | تعبير يحسب معرّف (بصمة) السلسلة الزمنية من وسومها. إذا لم يتم تعيينه، فسيُستخدم التعبير الافتراضي للعمود `id`. وإذا لم يكن التعبير الافتراضي للعمود `id` معيّنًا أيضًا، فسيتم اختيار التعبير تلقائيًا                       |
| `tags_to_columns`                    | Map        | {}                 | `Map` يحدد الوسوم التي يجب وضعها في أعمدة منفصلة في جدول [tags](#tags-table). الصياغة: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                                         |
| `use_all_tags_column_to_generate_id` | Bool       | true               | عند إنشاء تعبير لحساب معرّف سلسلة زمنية، يتيح هذا الخيار استخدام العمود `all_tags` في هذا الحساب                                                                                                                            |
| `store_min_time_and_max_time`        | Bool       | true               | إذا تم تعيينه إلى true، فسيخزّن الجدول `min_time` و`max_time` لكل سلسلة زمنية                                                                                                                                               |
| `aggregate_min_time_and_max_time`    | Bool       | true               | عند إنشاء جدول `tags` الداخلي الهدف، يتيح هذا الخيار استخدام `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` بدلًا من `Nullable(DateTime64(3))` فقط كنوع للعمود `min_time`، وينطبق الأمر نفسه على العمود `max_time` |
| `filter_by_min_time_and_max_time`    | Bool       | true               | إذا تم تعيينه إلى true، فسيستخدم الجدول العمودين `min_time` و`max_time` لتصفية السلاسل الزمنية                                                                                                                              |

<div id="functions">
  # الدوال
</div>

فيما يلي قائمة بالدوال التي تقبل جدول `TimeSeries` كوسيط:

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)