---
description: 'صُمِّمت محركات الجداول من عائلة `MergeTree` لمعدلات إدخال بيانات مرتفعة
  ولأحجام بيانات هائلة.'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'محرك الجدول MergeTree'
doc_type: 'مرجع'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # محرك الجدول MergeTree
</div>

يُعد المحرك `MergeTree` والمحركات الأخرى من عائلة `MergeTree` (مثل `ReplacingMergeTree` و`AggregatingMergeTree`) أكثر محركات الجداول استخدامًا وأكثرها موثوقية في ClickHouse.

صُممت محركات الجداول من عائلة `MergeTree` لمعدلات عالية من إدخال البيانات وأحجام بيانات هائلة.
وتُنشئ عمليات الإدراج أجزاء الجدول، ثم تدمجها عملية تعمل في الخلفية مع أجزاء جدول أخرى.

الميزات الرئيسية لمحركات الجداول من عائلة `MergeTree`.

* يحدد المفتاح الأساسي للجدول ترتيب الفرز داخل كل جزء جدول (فهرس عنقودي). كما أن المفتاح الأساسي لا يشير إلى صفوف فردية، بل إلى كتل من 8192 صفًا تُسمى حبيبات. وهذا يجعل المفاتيح الأساسية لمجموعات البيانات الضخمة صغيرة بما يكفي لتبقى محمّلة في الذاكرة الرئيسية، مع توفير وصول سريع إلى البيانات المخزنة على القرص.

* يمكن تقسيم الجداول باستخدام أي تعبير تقسيم. ويضمن استبعاد الأقسام تجاهل قراءة الأقسام عندما يسمح الاستعلام بذلك.

* يمكن تكرار البيانات عبر عدة عُقد في المجموعة لتحقيق التوافر العالي، والتحويل التلقائي عند الفشل، والترقيات دون توقف. راجع [تكرار البيانات](/ar/engines/table-engines/mergetree-family/replication.md).

* تدعم محركات الجداول `MergeTree` أنواعًا مختلفة من الإحصاءات وأساليب أخذ العينات للمساعدة في تحسين الاستعلامات.

:::note
على الرغم من تشابه الاسم، فإن محرك [Merge](/ar/engines/table-engines/special/merge) يختلف عن محركات `*MergeTree`.
:::

<div id="table_engine-mergetree-creating-a-table">
  ## إنشاء الجداول
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

للحصول على وصف تفصيلي للمعلمات، راجع تعليمة [CREATE TABLE](/ar/sql-reference/statements/create/table.md)

<div id="mergetree-query-clauses">
  ### عبارات الاستعلام
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — اسم المحرك ومعاملاته. ‏`ENGINE = MergeTree()`. لا يحتوي محرك ‏`MergeTree` على أي معاملات.

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — مفتاح الفرز.

Tuple يتألف من أسماء الأعمدة أو أي تعبيرات. مثال: `ORDER BY (CounterID + 1, EventDate)`.

إذا لم يتم تعريف مفتاح أساسي (أي لم يتم تحديد `PRIMARY KEY`)، يستخدم ClickHouse مفتاح الفرز بوصفه المفتاح الأساسي.

إذا لم يكن الفرز مطلوبًا، يمكنك استخدام الصيغة `ORDER BY tuple()`.
وبدلاً من ذلك، إذا كان الإعداد `create_table_empty_primary_key_by_default` مفعّلًا، تتم إضافة `ORDER BY ()` ضمنيًا إلى عبارات `CREATE TABLE`. راجع [اختيار مفتاح أساسي](#selecting-a-primary-key).

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — [مفتاح التقسيم](/ar/engines/table-engines/mergetree-family/custom-partitioning-key.md). وهو اختياري. في معظم الحالات، لا تحتاج إلى مفتاح تقسيم، وحتى إذا احتجت إلى التقسيم، فغالبًا لن تحتاج إلى مفتاح تقسيم أدق من مستوى الشهر. لا يسرّع التقسيم الاستعلامات (على خلاف تعبير ORDER BY). يجب ألّا تستخدم تقسيمًا شديد الدقة إطلاقًا. لا تقسّم بياناتك حسب معرّفات العملاء أو أسمائهم (واجعل بدلًا من ذلك معرّف العميل أو اسمه هو العمود الأول في تعبير ORDER BY).

للتقسيم حسب الشهر، استخدم التعبير `toYYYYMM(date_column)`، حيث إن `date_column` هو عمود يحتوي على تاريخ من النوع [Date](/ar/sql-reference/data-types/date.md). وتكون أسماء الأقسام هنا بالتنسيق `"YYYYMM"`.

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — المفتاح الأساسي إذا كان [يختلف عن مفتاح الفرز](#choosing-a-primary-key-that-differs-from-the-sorting-key). اختياري.

يؤدي تحديد مفتاح فرز (باستخدام عبارة `ORDER BY`) ضمنيًا إلى تحديد مفتاح أساسي.
وعادةً لا تكون هناك حاجة إلى تحديد المفتاح الأساسي إلى جانب مفتاح الفرز.

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — تعبير لأخذ العينات. اختياري.

إذا تم تحديده، فيجب أن يكون جزءًا من المفتاح الأساسي.
يجب أن يُرجِع تعبير أخذ العينات عددًا صحيحًا غير موقّع.

مثال: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

<div id="ttl">
  #### TTL
</div>

`TTL` — قائمة بالقواعد التي تحدد مدة تخزين الصفوف وآلية النقل التلقائي للأجزاء [بين الأقراص ووحدات التخزين](#table_engine-mergetree-multiple-volumes). اختياري.

يجب أن يُرجع التعبير قيمة من النوع `Date` أو `DateTime`، على سبيل المثال `TTL date + INTERVAL 1 DAY`.

يحدد نوع القاعدة `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` الإجراء الذي يجب تنفيذه على الجزء عند تحقق التعبير (أي عند بلوغه الوقت الحالي): حذف الصفوف منتهية الصلاحية، أو نقل جزء (إذا تحقق التعبير لجميع الصفوف في الجزء) إلى القرص المحدد (`TO DISK 'xxx'`) أو إلى وحدة التخزين (`TO VOLUME 'xxx'`)، أو تجميع القيم في الصفوف منتهية الصلاحية. النوع الافتراضي للقاعدة هو الحذف (`DELETE`). يمكن تحديد قائمة تضم عدة قواعد، لكن يجب ألا تزيد قواعد `DELETE` على واحدة.

لمزيد من التفاصيل، راجع [TTL للأعمدة والجداول](#table_engine-mergetree-ttl)

<div id="settings">
  #### الإعدادات
</div>

راجع [إعدادات MergeTree](../../../operations/settings/merge-tree-settings.md).

**مثال على إعداد Sections**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

في المثال، نضبط التقسيم حسب الشهر.

كما نضبط تعبيرًا لأخذ العينات كتجزئة تستند إلى معرّف المستخدم. يتيح لك ذلك توزيع البيانات في الجدول توزيعًا شبه عشوائي لكل من `CounterID` و`EventDate`. إذا حددت عبارة [SAMPLE](/ar/sql-reference/statements/select/sample) عند تحديد البيانات، فسيُرجع ClickHouse عينة بيانات شبه عشوائية ومتجانسة لمجموعة فرعية من المستخدمين.

يمكن حذف الإعداد `index_granularity` لأن 8192 هي القيمة الافتراضية.

<details markdown="1">
  <summary>الطريقة المهجورة لإنشاء جدول</summary>

  :::note
  لا تستخدم هذه الطريقة في المشاريع الجديدة. إذا أمكن، انقل المشاريع القديمة إلى الطريقة الموضحة أعلاه.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **معلمات MergeTree()**

  * `date-column` — اسم عمود من النوع [Date](/ar/sql-reference/data-types/date.md). ينشئ ClickHouse تلقائيًا أقسامًا حسب الشهر استنادًا إلى هذا العمود. تكون أسماء الأقسام بالتنسيق `"YYYYMM"`.
  * `sampling_expression` — تعبير لأخذ العينات.
  * `(primary, key)` — المفتاح الأساسي. النوع: [Tuple()](/ar/sql-reference/data-types/tuple.md)
  * `index_granularity` — دقة الفهرس. عدد صفوف البيانات بين &quot;marks&quot; الخاصة بالفهرس. القيمة 8192 مناسبة لمعظم المهام.

  **مثال**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  يُضبط المحرك `MergeTree` بالطريقة نفسها كما في المثال أعلاه لطريقة إعداد المحرك الرئيسية.
</details>

<div id="mergetree-data-storage">
  ## تخزين البيانات
</div>

يتكوّن الجدول من أجزاء بيانات مرتبة حسب المفتاح الأساسي.

عند إدخال البيانات في جدول، تُنشأ أجزاء بيانات منفصلة، ويُرتَّب كلٌّ منها ترتيبًا معجميًا حسب المفتاح الأساسي. على سبيل المثال، إذا كان المفتاح الأساسي هو `(CounterID, Date)`، فإن البيانات في الجزء تُرتَّب حسب `CounterID`، وضمن كل `CounterID`، تُرتَّب حسب `Date`.

تُفصل البيانات التابعة للتقسيمات المختلفة إلى أجزاء مختلفة. وفي الخلفية، يدمج ClickHouse أجزاء البيانات لجعل التخزين أكثر كفاءة. ولا تُدمج الأجزاء التابعة لتقسيمات مختلفة. كما أن آلية الدمج لا تضمن وجود جميع الصفوف التي لها المفتاح الأساسي نفسه في جزء البيانات نفسه.

يمكن تخزين أجزاء البيانات بتنسيق `Wide` أو `Compact`. في تنسيق `Wide` يُخزَّن كل عمود في ملف منفصل ضمن نظام ملفات، وفي تنسيق `Compact` تُخزَّن جميع الأعمدة في ملف واحد. ويمكن استخدام تنسيق `Compact` لتحسين أداء عمليات الإدخال الصغيرة والمتكررة.

يتحكم إعدادا `min_bytes_for_wide_part` و`min_rows_for_wide_part` الخاصان بمحرك الجدول في تنسيق تخزين البيانات. إذا كان عدد البايتات أو الصفوف في جزء البيانات أقل من قيمة الإعداد المقابل، فسيُخزَّن الجزء بتنسيق `Compact`. وإلا فسيُخزَّن بتنسيق `Wide`. وإذا لم يُضبط أيٌّ من هذه الإعدادات، فستُخزَّن أجزاء البيانات بتنسيق `Wide`.

يُقسَّم كل جزء بيانات منطقيًا إلى حبيبات. والحبيبة هي أصغر مجموعة بيانات غير قابلة للتجزئة يقرؤها ClickHouse عند اختيار البيانات. لا يقسم ClickHouse الصفوف أو القيم، لذلك تحتوي كل حبيبة دائمًا على عدد صحيح من الصفوف. ويُعلَّم الصف الأول في الحبيبة بقيمة المفتاح الأساسي لذلك الصف. ولكل جزء بيانات، ينشئ ClickHouse ملف فهرس يخزّن العلامات. ولكل عمود، سواء أكان ضمن المفتاح الأساسي أم لا، يخزّن ClickHouse أيضًا العلامات نفسها. وتتيح لك هذه العلامات العثور على البيانات مباشرةً في ملفات الأعمدة.

يُقيَّد حجم الحبيبة بإعدادي `index_granularity` و`index_granularity_bytes` الخاصين بمحرك الجدول. ويقع عدد الصفوف في الحبيبة ضمن النطاق `[1, index_granularity]`، اعتمادًا على حجم الصفوف. ويمكن أن يتجاوز حجم الحبيبة `index_granularity_bytes` إذا كان حجم صف واحد أكبر من قيمة الإعداد. وفي هذه الحالة، يكون حجم الحبيبة مساويًا لحجم الصف.

<div id="primary-keys-and-indexes-in-queries">
  ## المفاتيح الأساسية والفهارس في الاستعلامات
</div>

خذ المفتاح الأساسي `(CounterID, Date)` على سبيل المثال. في هذه الحالة، يمكن توضيح الترتيب والفهرس كما يلي:

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

إذا كان استعلام البيانات يحدّد ما يلي:

* `CounterID in ('a', 'h')`، يقرأ الخادم البيانات ضمن نطاقات العلامات `[0, 3)` و`[6, 8)`.
* `CounterID IN ('a', 'h') AND Date = 3`، يقرأ الخادم البيانات ضمن نطاقات العلامات `[1, 3)` و`[7, 8)`.
* `Date = 3`، يقرأ الخادم البيانات ضمن نطاق العلامات `[1, 10]`.

تُظهر الأمثلة أعلاه أن استخدام الفهرس يكون دائمًا أكثر فعالية من المسح الكامل.

يسمح الفهرس المتناثر بقراءة بيانات إضافية. عند قراءة نطاق واحد من المفتاح الأساسي، قد تتم قراءة ما يصل إلى `index_granularity * 2` من الصفوف الإضافية في كل كتلة بيانات.

تتيح لك الفهارس المتناثرة العمل مع عدد كبير جدًا من صفوف الجدول، لأن هذه الفهارس، في معظم الحالات، تتسع في ذاكرة RAM الخاصة بالحاسوب.

لا يتطلب ClickHouse مفتاحًا أساسيًا فريدًا. يمكنك إدراج عدة صفوف بالمفتاح الأساسي نفسه.

يمكنك استخدام تعبيرات من النوع `Nullable` في عبارتي `PRIMARY KEY` و`ORDER BY`، لكن لا يُنصح بذلك بشدة. للسماح بهذه الميزة، فعِّل الإعداد [allow&#95;nullable&#95;key](/ar/operations/settings/merge-tree-settings/#allow_nullable_key). وينطبق مبدأ [NULLS&#95;LAST](/ar/sql-reference/statements/select/order-by.md/#sorting-of-special-values) على قيم `NULL` في عبارة `ORDER BY`.

<div id="selecting-a-primary-key">
  ### اختيار المفتاح الأساسي
</div>

عدد الأعمدة في المفتاح الأساسي غير مقيَّد صراحةً. ووفقًا لبنية البيانات، يمكنك تضمين عدد أكبر أو أقل من الأعمدة في المفتاح الأساسي. وقد يترتب على ذلك ما يلي:

* تحسين أداء الفهرس.

  إذا كان المفتاح الأساسي هو `(a, b)`، فإن إضافة عمود آخر `c` ستحسن الأداء إذا تحققت الشروط التالية:

  * توجد استعلامات تتضمن شرطًا على العمود `c`.
  * تشيع نطاقات بيانات طويلة (أطول عدة مرات من `index_granularity`) ذات قيم متطابقة لـ `(a, b)`. وبعبارة أخرى، عندما تتيح لك إضافة عمود آخر تخطي نطاقات بيانات طويلة نسبيًا.

* تحسين ضغط البيانات.

  يرتّب ClickHouse البيانات بحسب المفتاح الأساسي، لذا كلما زاد التجانس كان الضغط أفضل.

* توفير منطق إضافي عند دمج أجزاء البيانات في محركي [CollapsingMergeTree](/ar/engines/table-engines/mergetree-family/collapsingmergetree) و[SummingMergeTree](/ar/engines/table-engines/mergetree-family/summingmergetree.md).

  في هذه الحالة، قد يكون من المنطقي تحديد *مفتاح الفرز* بحيث يختلف عن المفتاح الأساسي.

سيؤثر المفتاح الأساسي الطويل سلبًا على أداء الإدراج واستهلاك الذاكرة، لكن الأعمدة الإضافية في المفتاح الأساسي لا تؤثر على أداء ClickHouse أثناء استعلامات `SELECT`.

يمكنك إنشاء جدول بدون مفتاح أساسي باستخدام الصياغة `ORDER BY tuple()`. في هذه الحالة، يخزّن ClickHouse البيانات وفق ترتيب إدراجها. إذا كنت تريد الحفاظ على ترتيب البيانات عند إدراجها بواسطة استعلامات `INSERT ... SELECT`، فاضبط [max&#95;insert&#95;threads = 1](/ar/operations/settings/settings#max_insert_threads).

لاختيار البيانات بالترتيب الأصلي، استخدم استعلامات `SELECT` [أحادية الخيط](/ar/operations/settings/settings.md/#max_threads).

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### اختيار مفتاح أساسي يختلف عن مفتاح الفرز
</div>

يمكن تحديد مفتاح أساسي (تعبير يتضمن قيماً تُكتب في ملف الفهرس لكل علامة) يختلف عن مفتاح الفرز (تعبير لفرز الصفوف في أجزاء البيانات). في هذه الحالة، يجب أن تكون مجموعة تعبير المفتاح الأساسي بادئةً لمجموعة تعبير مفتاح الفرز.

تكون هذه الميزة مفيدة عند استخدام محركي الجداول [SummingMergeTree](/ar/engines/table-engines/mergetree-family/summingmergetree.md) و
[AggregatingMergeTree](/ar/engines/table-engines/mergetree-family/aggregatingmergetree.md). في الحالة الشائعة عند استخدام هذين المحركين، يحتوي الجدول على نوعين من الأعمدة: *الأبعاد* و*المقاييس*. وعادةً ما تجمع الاستعلامات قيم أعمدة المقاييس باستخدام `GROUP BY` عشوائي، مع التصفية حسب الأبعاد. ونظراً إلى أن SummingMergeTree وAggregatingMergeTree يجمعان الصفوف التي لها القيمة نفسها لمفتاح الفرز، فمن الطبيعي إضافة جميع الأبعاد إليه. ونتيجةً لذلك، يتكوّن تعبير المفتاح من قائمة طويلة من الأعمدة، ويجب تحديث هذه القائمة باستمرار مع إضافة أبعاد جديدة.

في هذه الحالة، من المنطقي الإبقاء على عدد قليل فقط من الأعمدة في المفتاح الأساسي لتوفير عمليات مسح نطاق فعّالة، وإضافة بقية أعمدة الأبعاد إلى مجموعة مفتاح الفرز.

يُعد [ALTER](/ar/sql-reference/statements/alter/index.md) لمفتاح الفرز عملية خفيفة، لأنه عند إضافة عمود جديد في الوقت نفسه إلى الجدول وإلى مفتاح الفرز، لا تحتاج أجزاء البيانات الحالية إلى أي تغيير. وبما أن مفتاح الفرز القديم هو بادئة لمفتاح الفرز الجديد، ولا توجد بيانات في العمود المضاف حديثاً، فإن البيانات تكون مرتبة وفقاً لمفتاحي الفرز القديم والجديد في لحظة تعديل الجدول.

<div id="use-of-indexes-and-partitions-in-queries">
  ### استخدام الفهارس والتقسيمات في الاستعلامات
</div>

بالنسبة إلى استعلامات `SELECT`، يحلّل ClickHouse ما إذا كان يمكن استخدام فهرس. ويمكن استخدام الفهرس إذا كانت عبارة `WHERE/PREWHERE` تحتوي على تعبير (إما كأحد عناصر الربط أو بالكامل) يمثّل عملية مقارنة مساواة أو عدم مساواة، أو إذا كانت تحتوي على `IN` أو `LIKE` مع بادئة ثابتة على الأعمدة أو التعابير الموجودة في المفتاح الأساسي أو مفتاح التقسيم، أو على بعض الدوال الجزئية التكرار لهذه الأعمدة، أو على العلاقات المنطقية بين هذه التعابير.

وبذلك، يمكن تنفيذ الاستعلامات بسرعة على نطاق واحد أو عدة نطاقات من المفتاح الأساسي. في هذا المثال، ستكون الاستعلامات سريعة عند تشغيلها لعلامة تتبّع معيّنة، أو لعلامة معيّنة مع نطاق تواريخ محدد، أو لعلامة معيّنة وتاريخ محدد، أو لعدة علامات مع نطاق تواريخ، وهكذا.

لنلقِ نظرة على المحرك المُهيأ كما يلي:

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

في هذه الحالة، ضمن الاستعلامات:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

سيستخدم ClickHouse فهرس المفتاح الأساسي لاستبعاد البيانات غير المطابقة، ومفتاح التقسيم الشهري لاستبعاد الأقسام الواقعة ضمن نطاقات تاريخ غير مناسبة.

تُظهر الاستعلامات أعلاه أن الفهرس يُستخدم حتى مع التعبيرات المعقدة. وتُنظَّم القراءة من الجدول بحيث لا يكون استخدام الفهرس أبطأ من المسح الكامل.

في المثال أدناه، لا يمكن استخدام الفهرس.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

للتحقق مما إذا كان ClickHouse يستطيع استخدام الفهرس عند تنفيذ استعلام، استخدم الإعدادين [force&#95;index&#95;by&#95;date](/ar/operations/settings/settings.md/#force_index_by_date) و[force&#95;primary&#95;key](/ar/operations/settings/settings#force_primary_key).

يتيح مفتاح التقسيم حسب الشهر قراءة كتل البيانات التي تحتوي فقط على تواريخ ضمن النطاق المطلوب. وفي هذه الحالة، قد تحتوي كتلة البيانات على بيانات تخص عدة تواريخ (حتى شهر كامل). وداخل الكتلة، تكون البيانات مرتبة حسب المفتاح الأساسي، الذي قد لا يكون التاريخ فيه العمود الأول. لذلك، فإن استخدام استعلام يتضمن شرطًا على التاريخ فقط من دون تحديد بادئة المفتاح الأساسي سيؤدي إلى قراءة بيانات أكثر مقارنةً بالاستعلام الخاص بتاريخ واحد.

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### استخدام الفهرس للتعبيرات الحتمية في المفاتيح الأساسية
</div>

يمكن أن يحتوي المفتاح الأساسي على تعبيرات، وليس على أسماء الأعمدة فقط. ولا تقتصر هذه التعبيرات على سلاسل دوال بسيطة، بل يمكن أن تكون أشجار تعبيرات بأي شكل (على سبيل المثال، دوال متداخلة وتعبيرات مركبة)، ما دامت حتمية.

يكون التعبير **حتميًا** إذا كان يعيد دائمًا النتيجة نفسها لقيم الإدخال نفسها (على سبيل المثال: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`؛ بخلاف `now()` أو `rand()`). وإذا كان المفتاح الأساسي يحتوي على تعبيرات حتمية، يمكن لـ ClickHouse تطبيقها على القيم الثابتة في الاستعلام واستخدام النتيجة لبناء شروط على فهرس المفتاح الأساسي. ويتيح ذلك تخطي البيانات لشروط التصفية مثل `=`, `IN`, و `has`.

ومن حالات الاستخدام الشائعة إبقاء المفتاح الأساسي مضغوطًا (مثل تخزين hash بدلًا من `String` طويل)، مع الاستمرار في تمكين شروط التصفية على العمود الأصلي من استخدام الفهرس.

مثال على مفتاح أساسي حتمي (لكن غير حقني):

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

أمثلة على شروط التصفية التي يمكنها استخدام الفهرس:

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

في هذه الحالات، يحسب ClickHouse ‎`length('alice')`‎ (والثوابت الأخرى) مرةً واحدة، ويستخدم قيم الطول لتضييق النطاقات في فهرس المفتاح الأساسي. ونظرًا إلى أن طول السلسلة **ليس حقنيًا**، فقد تتشارك سلاسل ‎`user_id`‎ مختلفة في الطول نفسه، لذلك قد يقرأ الفهرس حبيبات إضافية (إيجابيات كاذبة). ومع ذلك، تبقى النتيجة صحيحة لأن الشرط الأصلي ‎(`user_id = ...`‎ و‎`IN`‎ وما إلى ذلك) يظل مطبَّقًا بعد القراءة.

إذا كان التعبير الحتمي أيضًا **حقنيًا** (أي إن المدخلات المختلفة لا يمكن أن تنتج المخرَج نفسه لأنواع الوسيطات المستخدمة)، فيمكن لـ ClickHouse أيضًا استخدام الفهرس بفعالية مع الصيغ المنفية: ‎`!=`‎ و‎`NOT IN`‎ و‎`NOT has(...)`‎. على سبيل المثال، ‎`reverse(p)`‎ و‎`hex(p)`‎ دالتان حقنيتان بالنسبة إلى ‎`String`‎.

مثال على مفتاح أساسي حقني:

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

تُدعَم أيضًا التعبيرات الحقنية الأكثر تعقيدًا، مثل:

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

أمثلة على شروط التصفية التي يمكن أن تستخدم الفهرس:

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### استخدام الفهرس مع المفاتيح الأساسية الرتيبة جزئيًا
</div>

لنأخذ، على سبيل المثال، أيام الشهر. فهي تشكّل [متتالية رتيبة](https://en.wikipedia.org/wiki/Monotonic_function) ضمن شهر واحد، لكنها لا تكون رتيبة عبر فترات زمنية أطول. هذه متتالية رتيبة جزئيًا. إذا أنشأ المستخدم جدولًا بمفتاح أساسي رتيب جزئيًا، فإن ClickHouse ينشئ فهرسًا متناثرًا كالمعتاد. وعندما يستعلم المستخدم عن بيانات من هذا النوع من الجداول، يحلّل ClickHouse شروط الاستعلام. وإذا أراد المستخدم الحصول على بيانات بين علامتَي فهرسة، وكانت كلتاهما تقعان ضمن شهر واحد، فيمكن لـ ClickHouse استخدام الفهرس في هذه الحالة تحديدًا، لأنه يستطيع حساب المسافة بين معاملات الاستعلام وعلامات الفهرس.

لا يمكن لـ ClickHouse استخدام الفهرس إذا كانت قيم المفتاح الأساسي ضمن نطاق معاملات الاستعلام لا تمثّل متتالية رتيبة. في هذه الحالة، يستخدم ClickHouse أسلوب الفحص الكامل.

يستخدم ClickHouse هذا المنطق ليس فقط مع متتاليات أيام الشهر، بل مع أي مفتاح أساسي يمثّل متتالية رتيبة جزئيًا.

<div id="table_engine-mergetree-data_skipping-indexes">
  ### فهارس تخطي البيانات
</div>

يوجد تعريف الفهرس في قسم الأعمدة ضمن استعلام `CREATE`.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

بالنسبة إلى الجداول من عائلة `*MergeTree`، يمكن تحديد فهارس تخطي البيانات.

تُجمِّع هذه الفهارس بعض المعلومات حول الـ expression المحدد على الكتل، والتي تتكوّن من `granularity_value` من الحبيبات (ويُحدَّد حجم الحبيبة باستخدام الإعداد `index_granularity` في محرك الجدول). ثم تُستخدَم هذه التجميعات في استعلامات `SELECT` لتقليل كمية البيانات المطلوب قراءتها من القرص، وذلك من خلال تخطي كتل كبيرة من البيانات التي لا يمكن فيها استيفاء استعلام `where`.

يمكن حذف عبارة `GRANULARITY`، والقيمة الافتراضية لـ `granularity_value` هي 1.

**مثال**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

يمكن لـ ClickHouse الاستفادة من الفهارس الواردة في المثال لتقليل كمية البيانات المقروءة من القرص في الاستعلامات التالية:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

يمكن أيضًا إنشاء فهارس تخطي البيانات على الأعمدة المركبة:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### أنواع فهارس التخطي
</div>

يدعم محرك الجداول `MergeTree` الأنواع التالية من فهارس التخطي.
لمزيد من المعلومات حول كيفية استخدام فهارس التخطي لتحسين الأداء،
راجع [&quot;فهم فهارس تخطي البيانات في ClickHouse&quot;](/ar/optimize/skipping-indexes).

* فهرس [`MinMax`](#minmax)
* فهرس [`Set`](#set)
* فهرس [`bloom_filter`](#bloom-filter)
* فهرس [`ngrambf_v1`](#n-gram-bloom-filter) *(مهمل)*
* فهرس [`tokenbf_v1`](#token-bloom-filter) *(مهمل)*
* فهرس [`text`](#text)
* فهرس [`vector_similarity`](#vector-similarity)

<div id="minmax">
  #### فهرس التخطي MinMax
</div>

لكل حبيبة فهرس، تُخزَّن القيمتان الصغرى والكبرى لتعبيرٍ ما.
(إذا كان التعبير من النوع `tuple`، فسيُخزِّن القيمتين الصغرى والكبرى لكل عنصر من عناصر `tuple`.)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

لكل index حبيبة، يُخزَّن بحد أقصى `max_rows` من القيم الفريدة للتعبير المحدد.
ويعني `max_rows = 0` &quot;تخزين جميع القيم الفريدة&quot;.

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### مرشح بلوم
</div>

يُخزَّن [مرشح بلوم](https://en.wikipedia.org/wiki/Bloom_filter) للأعمدة المحددة لكل index حبيبة.

```text title="Syntax"
bloom_filter([false_positive_rate])
```

يمكن أن تأخذ المعلمة `false_positive_rate` قيمة بين 0 و1 (القيمة الافتراضية: `0.025`)، وتحدّد احتمال توليد نتيجة إيجابية، مما يزيد كمية البيانات التي يجب قراءتها.

أنواع البيانات التالية مدعومة:

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note نوع بيانات Map: تحديد إنشاء الفهرس باستخدام المفاتيح أو القيم
بالنسبة إلى نوع البيانات `Map`، يمكن للعميل تحديد ما إذا كان ينبغي إنشاء الفهرس للمفاتيح أو للقيم باستخدام الدالتين [`mapKeys`](/ar/sql-reference/functions/tuple-map-functions.md/#mapKeys) أو [`mapValues`](/ar/sql-reference/functions/tuple-map-functions.md/#mapValues).
:::

:::note نوع بيانات JSON: فهرسة مسارات JSON
بالنسبة إلى نوع البيانات [`JSON`](/ar/sql-reference/data-types/newjson)، يمكن إنشاء فهرس مرشح بلوم على مجموعة المسارات باستخدام الدالة [`JSONAllPaths`](/ar/sql-reference/functions/json-functions#JSONAllPaths). يتيح ذلك تخطي الحبيبات التي لا يتوفّر فيها مسار JSON المطلوب في الاستعلام. راجع [فهارس تخطي البيانات لـ JSON](/ar/sql-reference/data-types/newjson#data-skipping-indexes-for-json) لمزيد من التفاصيل.
:::

<div id="n-gram-bloom-filter">
  #### مرشح بلوم لـ N-gram *(مهمل)*
</div>

:::note
مع الإتاحة العامة (GA) لفهرس `text` بدءًا من إصدار ClickHouse 26.2، لم يعد الفهرس `ngrambf_v1` موصىً به للبحث النصي الكامل.

راجع صفحة [&quot;البحث النصي الكامل باستخدام فهارس النص&quot;](./textindexes.md) لمزيد من التفاصيل.
:::

يخزّن لكل index حبيبة [مرشح بلوم](https://en.wikipedia.org/wiki/Bloom_filter) لـ [n-grams](https://en.wikipedia.org/wiki/N-gram) الخاصة بالأعمدة المحددة.

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| المعلمة                         | الوصف                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------------- |
| `n`                             | حجم ngram                                                                                           |
| `size_of_bloom_filter_in_bytes` | حجم مرشح بلوم بالبايت. يمكنك استخدام قيمة كبيرة هنا، مثل `256` أو `512`، لأنه قابل للضغط بكفاءة. |
| `number_of_hash_functions`      | عدد دوال hash المستخدمة في مرشح بلوم.                                                            |
| `random_seed`                   | البذرة المستخدمة لدوال hash الخاصة بـ مرشح بلوم.                                                 |

لا يعمل هذا الفهرس إلا مع أنواع البيانات التالية:

* [`String`](/ar/sql-reference/data-types/string.md)
* [`FixedString`](/ar/sql-reference/data-types/fixedstring.md)
* [`Map`](/ar/sql-reference/data-types/map.md)

لتقدير معلمات `ngrambf_v1`، يمكنك استخدام [الدوال المعرّفة من قبل المستخدم (UDFs)](/ar/sql-reference/statements/create/function.md) التالية.

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

لاستخدام هذه الدوال، تحتاج إلى تحديد معاملين على الأقل:

* `total_number_of_all_grams`
* `probability_of_false_positives`

على سبيل المثال، يوجد `4300` ngrams في الـحبيبة، وتتوقع أن تكون الإيجابيات الكاذبة أقل من `0.0001`.
بعد ذلك، يمكن تقدير المعاملات الأخرى بتنفيذ الاستعلامات التالية:

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

وبالطبع، يمكنك أيضًا استخدام هذه الدوال لتقدير المعلمات لحالات أخرى.
وتستند الدوال المذكورة أعلاه إلى حاسبة مرشح بلوم [هنا](https://hur.st/bloomfilter).

<div id="token-bloom-filter">
  #### مرشح بلوم للرموز
</div>

:::note
مع الإتاحة العامة (GA) لفهرس `text` بدءًا من إصدار ClickHouse 26.2، لم يعد يُنصح باستخدام الفهرس `tokenbf_v1` لإجراء البحث النصي الكامل.

راجع صفحة [&quot;البحث النصي الكامل باستخدام فهارس النص&quot;](./textindexes.md) لمزيد من التفاصيل.
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### مرشح بلوم لـ sparse grams
</div>

يشبه مرشح بلوم لـ sparse grams المرشح `ngrambf_v1`، لكنه يستخدم [رموز sparse grams](/ar/sql-reference/functions/string-functions.md/#sparseGrams) بدلًا من ngrams.

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### الفهرس النصي
</div>

ينشئ فهرسًا معكوسًا لبيانات نصية مُجزأة إلى رموز، مما يتيح البحث النصي الكامل بكفاءة وبشكل حتمي. راجع [هنا](textindexes.md) للتفاصيل.

<div id="vector-similarity">
  #### تشابه المتجهات
</div>

يدعم البحث التقريبي عن أقرب الجيران؛ راجع [هنا](annindexes.md) لمزيد من التفاصيل.

<div id="functions-support">
  ### دعم الدوال
</div>

تتضمن الشروط في عبارة `WHERE` استدعاءاتٍ لدوال تعمل على الأعمدة. وإذا كان العمود جزءًا من فهرس، يحاول ClickHouse استخدام هذا الفهرس عند تنفيذ هذه الدوال. يدعم ClickHouse مجموعات فرعية مختلفة من الدوال عند استخدام الفهارس.

يمكن استخدام الفهارس من النوع `set` مع جميع الدوال. أما أنواع الفهارس الأخرى، فدعمها يكون على النحو التالي:

| الدالة (المعامل) / الفهرس                                                                                                 | المفتاح الأساسي | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | text |
| ------------------------------------------------------------------------------------------------------------------------- | --------------- | ------ | -------------- | -------------- | ---------------- | ---------------- | ---- |
| [equals (=, ==)](/ar/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔               | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notEquals(!=, &lt;&gt;)](/ar/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔               | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [like](/ar/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔               | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [notLike](/ar/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔               | ✔      | ✔              | ✔              | ✗                | ✔                | ✗    |
| [match](/ar/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗               | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [startsWith](/ar/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔               | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [endsWith](/ar/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗               | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [multiSearchAny](/ar/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗               | ✗      | ✔              | ✗              | ✗                | ✗                | ✔    |
| [multiSearchAnyUTF8](/ar/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [multiMatchAny](/ar/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [in](/ar/sql-reference/functions/in-functions)                                                                               | ✔               | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notIn](/ar/sql-reference/functions/in-functions)                                                                            | ✔               | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [less (`<`)](/ar/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greater (`>`)](/ar/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [lessOrEquals (`<=`)](/ar/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greaterOrEquals (`>=`)](/ar/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [empty](/ar/sql-reference/functions/array-functions/#empty)                                                                  | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [notEmpty](/ar/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗               | ✔      | ✗              | ✗              | ✗                | ✔                | ✗    |
| [has](/ar/sql-reference/functions/array-functions#has)                                                                       | ✔               | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [hasAny](/ar/sql-reference/functions/array-functions#hasAny)                                                                 | ✗               | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasAll](/ar/sql-reference/functions/array-functions#hasAll)                                                                 | ✗               | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasToken](/ar/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗               | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenOrNull](/ar/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗               | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenCaseInsensitive (`*`)](/ar/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗               | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/ar/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗               | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasAnyTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [hasAllTokens](/ar/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [pointInPolygon](/ar/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔               | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [mapContains (mapContainsKey)](/ar/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsKeyLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValue](/ar/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValueLike](/ar/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗               | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |

لا يمكن لـ `ngrambf_v1` استخدام الدوال التي تحتوي على وسيطة ثابتة أصغر من حجم ngram في تحسين الاستعلام.

(*) لكي تكون `hasTokenCaseInsensitive` و`hasTokenCaseInsensitiveOrNull` فعّالتين، يجب إنشاء الفهرس `tokenbf_v1` على بيانات محوّلة إلى أحرف صغيرة، على سبيل المثال `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
قد تعطي مرشحات بلوم تطابقات إيجابية كاذبة، لذلك لا يمكن استخدام فهارس `ngrambf_v1` و`tokenbf_v1` و`sparse_grams` و`bloom_filter` لتحسين الاستعلامات التي يُتوقع أن تكون نتيجة الدالة فيها false.

على سبيل المثال:

* يمكن تحسينها:
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* لا يمكن تحسينها:
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## الإسقاطات
</div>

تشبه الإسقاطات [العروض المادية](/ar/sql-reference/statements/create/view)، لكنها تُعرَّف على مستوى الأجزاء. وهي توفّر ضمانات للاتساق، كما تُستخدَم تلقائيًا في الاستعلامات.

:::note
عند تطبيق الإسقاطات، ينبغي أيضًا مراعاة إعداد [force&#95;optimize&#95;projection](/ar/operations/settings/settings#force_optimize_projection).
:::

لا تُدعَم الإسقاطات في عبارات `SELECT` التي تستخدم المُعدِّل [FINAL](/ar/sql-reference/statements/select/from#final-modifier).

<div id="projection-query">
  ### استعلام الإسقاط
</div>

استعلام الإسقاط هو ما يعرّف الإسقاط. وهو يختار البيانات ضمنيًا من الجدول الأصل.
**الصيغة**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

يمكن تعديل الإسقاطات أو إسقاطها باستخدام تعليمة [ALTER](/ar/sql-reference/statements/alter/projection.md).

<div id="projection-index">
  ### فهارس الإسقاط
</div>

تُوسّع فهارس الإسقاط النظام الفرعي للإسقاط عبر توفير طريقة خفيفة وصريحة لتعريف فهارس على مستوى الإسقاط.
ومن الناحية الظاهرية، يظل فهرس الإسقاط إسقاطًا، ولكن بصياغة أبسط ومقصود أوضح: فهو يعرّف تعبيرًا مخصصًا للتصفية، بدلًا من استخدامه لخدمة بيانات مُخزَّنة ماديًا.
أما داخليًا، فلا يقوم فهرس الإسقاط بتخزين الجدول الأصلي ماديًا بترتيب صفوف مُعاد كما يفعل الإسقاط العادي.
وبدلًا من ذلك، يُخزَّن هذا الترتيب في صورة عمود تبديل رقمي `_part_offset`، أي `SELECT _part_offset ORDER BY <index_expr>`.

<div id="projection-index-syntax">
  #### البنية
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

مثال:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### أنواع الفهارس
</div>

الأنواع المدعومة حاليًا:

* **basic**: يكافئ فهرسًا عاديًا في MergeTree على التعبير.

يتيح هذا الإطار إضافة المزيد من أنواع الفهارس مستقبلًا.

<div id="projection-storage">
  ### تخزين الإسقاطات
</div>

تُخزَّن الإسقاطات داخل دليل الجزء. وهي تشبه الفهرس، لكنها تحتوي على دليل فرعي يخزّن جزءًا من جدول `MergeTree` دون اسم. وينشأ هذا الجدول من استعلام تعريف الإسقاط. إذا وُجدت عبارة `GROUP BY`، يصبح محرك التخزين الأساسي [AggregatingMergeTree](aggregatingmergetree.md)، وتُحوَّل جميع الدوال التجميعية إلى `AggregateFunction`. وإذا وُجدت عبارة `ORDER BY`، يستخدمها جدول `MergeTree` كتعبير المفتاح الأساسي. وأثناء عملية الدمج، يُدمَج جزء الإسقاط عبر آلية الدمج الخاصة بتخزينه. كما تُدمَج قيمة التحقق لجزء الجدول الأصل مع جزء الإسقاط. أما مهام الصيانة الأخرى فهي مشابهة لفهارس التخطي.

<div id="projection-query-analysis">
  ### تحليل الاستعلام
</div>

1. تحقّق مما إذا كان يمكن استخدام الإسقاط للإجابة عن الاستعلام المطلوب، أي إنه يُنتج النتيجة نفسها التي يُنتجها الاستعلام على الجدول الأساسي.
2. اختر أفضل تطابق ممكن، وهو التطابق الذي يتطلب قراءة أقل عدد من الحبيبات.
3. يختلف مسار تنفيذ الاستعلام الذي يستخدم الإسقاطات عن المسار الذي يستخدم الأجزاء الأصلية. وإذا كان الإسقاط غير موجود في بعض الأجزاء، فيمكننا إضافة مسار التنفيذ لـ&quot;إسقاطه&quot; أثناء التشغيل.

<div id="concurrent-data-access">
  ## الوصول المتزامن إلى البيانات
</div>

نستخدم تعدد الإصدارات للوصول المتزامن إلى الجداول. وبعبارة أخرى، عند قراءة الجدول وتحديثه في الوقت نفسه، تُقرأ البيانات من مجموعة الأجزاء السارية وقت تنفيذ الاستعلام. ولا توجد أقفال طويلة الأمد. كما أن عمليات الإدراج لا تعيق عمليات القراءة.

تتوازى القراءة من الجدول تلقائيًا.

<div id="table_engine-mergetree-ttl">
  ## TTL للأعمدة والجداول
</div>

يحدّد مدة بقاء القيم.

يمكن تعيين عبارة `TTL` للجدول بالكامل ولكل عمود على حدة. كما يمكن لـ `TTL` على مستوى الجدول تحديد آلية النقل التلقائي للبيانات بين الأقراص ووحدات التخزين، أو إعادة ضغط الأجزاء التي انتهت صلاحية جميع البيانات فيها.

يجب أن تُرجِع التعبيرات قيمة من نوع البيانات [Date](/ar/sql-reference/data-types/date.md) أو [Date32](/ar/sql-reference/data-types/date32.md) أو [DateTime](/ar/sql-reference/data-types/datetime.md) أو [DateTime64](/ar/sql-reference/data-types/datetime64.md).

:::tip[تجنب الدوال غير الحتمية في تعبيرات TTL]
يُقيَّم TTL أثناء عمليات الدمج في الخلفية، وليس عند الإدراج.
تُعاد تقييم دوال مثل `rand()` و`now()` و`now64()` عند كل عملية دمج، مما يؤدي إلى سلوك حذف غير متوقع.
يمنع ClickHouse التعبيرات التي لا تعتمد إطلاقًا على أي عمود، لكنه لا يرفض حاليًا الدوال غير الحتمية الممزوجة بمرجع إلى عمود (مثل `ts + rand()`). يجب أن تستند تعبيرات TTL فقط إلى قيم حتمية مشتقة من الأعمدة للحصول على نتائج متوقعة.
:::

**البنية**

تعيين مدة البقاء لعمود:

```sql
TTL time_column
TTL time_column + interval
```

لتعريف `interval`، استخدم معاملات [الفواصل الزمنية](/ar/sql-reference/operators#operators-for-working-with-dates-and-times)، على سبيل المثال:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### `TTL` للعمود
</div>

عندما تنتهي صلاحية القيم في العمود، يستبدلها ClickHouse بالقيم الافتراضية لنوع بيانات العمود. وإذا انتهت صلاحية جميع قيم العمود في جزء البيانات، يحذف ClickHouse هذا العمود من جزء البيانات في نظام الملفات.

لا يمكن استخدام عبارة `TTL` مع أعمدة المفتاح.

**أمثلة**

<div id="creating-a-table-with-ttl">
  #### إنشاء جدول باستخدام `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### إضافة TTL إلى أحد أعمدة جدول موجود
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### تعديل TTL الخاص بالعمود
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### TTL على مستوى الجدول
</div>

يمكن أن يحتوي الجدول على تعبير لحذف الصفوف منتهية الصلاحية، وعلى عدة تعبيرات لنقل الأجزاء تلقائيًا بين [الأقراص أو وحدات التخزين](#table_engine-mergetree-multiple-volumes). عند انتهاء صلاحية صفوف الجدول، يحذف ClickHouse جميع الصفوف المقابلة. أمّا بالنسبة إلى نقل الأجزاء أو إعادة ضغطها، فيجب أن تستوفي جميع صفوف الجزء معايير تعبير `TTL`.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

قد يلي كل تعبير `TTL` نوعُ قاعدة `TTL`. ويحدد هذا الإجراء الذي سيُنفَّذ عند استيفاء التعبير (أي عند وصوله إلى الوقت الحالي):

* `DELETE` - حذف الصفوف منتهية الصلاحية (الإجراء الافتراضي)؛
* `RECOMPRESS codec_name` - إعادة ضغط جزء البيانات باستخدام `codec_name`؛
* `TO DISK 'aaa'` - نقل الجزء إلى القرص `aaa`؛
* `TO VOLUME 'bbb'` - نقل الجزء إلى وحدة التخزين `bbb`؛
* `GROUP BY` - تجميع الصفوف منتهية الصلاحية.

يمكن استخدام الإجراء `DELETE` مع عبارة `WHERE` لحذف بعض الصفوف منتهية الصلاحية فقط استنادًا إلى شرط تصفية:

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

يجب أن يكون تعبير `GROUP BY` بادئة للمفتاح الأساسي للجدول.

إذا لم يكن العمود جزءًا من تعبير `GROUP BY` ولم يُعيَّن صراحةً في عبارة `SET`، فسيحتوي صف النتيجة على قيمة عشوائية من الصفوف المجمَّعة (كما لو طُبِّقت عليه الدالة التجميعية `any`).

**أمثلة**

<div id="creating-a-table-with-ttl">
  #### إنشاء جدول باستخدام `TTL`:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### تعديل `TTL` للجدول:
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

إنشاء جدول، تنتهي صلاحية صفوفه بعد شهر واحد. وتُحذف الصفوف المنتهية الصلاحية التي توافق تواريخها يوم الاثنين:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### إنشاء جدول يُعاد فيه ضغط الصفوف المنتهية الصلاحية:
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

إنشاء جدول تُجمَّع فيه الصفوف منتهية الصلاحية. في صفوف النتائج، يحتوي `x` على القيمة القصوى عبر الصفوف المجمَّعة، و`y` — القيمة الدنيا، و`d` — أي قيمة عشوائية من الصفوف المجمَّعة.

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### إزالة البيانات منتهية الصلاحية
</div>

تُزال البيانات التي انتهت صلاحية `TTL` الخاصة بها عندما يدمج ClickHouse أجزاء البيانات.

عندما يكتشف ClickHouse أن البيانات انتهت صلاحيتها، فإنه يُجري عملية دمج خارج الجدول الزمني المعتاد. وللتحكم في وتيرة عمليات الدمج هذه، يمكنك ضبط `merge_with_ttl_timeout`. وإذا كانت القيمة منخفضة جدًا، فسيُجري عددًا كبيرًا من عمليات الدمج خارج الجدول الزمني المعتاد، ما قد يستهلك الكثير من الموارد.

إذا نفّذت استعلام `SELECT` بين عمليات الدمج، فقد تحصل على بيانات منتهية الصلاحية. لتجنب ذلك، استخدم استعلام [OPTIMIZE](/ar/sql-reference/statements/optimize.md) قبل `SELECT`.

**راجع أيضًا**

* إعداد [ttl&#95;only&#95;drop&#95;parts](/ar/operations/settings/merge-tree-settings#ttl_only_drop_parts)

<div id="disk-types">
  ## أنواع الأقراص
</div>

بالإضافة إلى أجهزة التخزين الكتلية المحلية، يدعم ClickHouse أنواع التخزين التالية:

* [`s3` لـ S3 وMinIO](#table_engine-mergetree-s3)
* [`gcs` لـ GCS](/ar/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` لـ Azure Blob Storage](/ar/operations/storing-data#azure-blob-storage)
* [`hdfs` لـ HDFS](/ar/engines/table-engines/integrations/hdfs)
* [`web` للقراءة فقط من الويب](/ar/operations/storing-data#web-storage)
* [`cache` للتخزين المؤقت المحلي](/ar/operations/storing-data#using-local-cache)
* [`s3_plain` للنسخ الاحتياطية إلى S3](/ar/operations/backup/disk)
* [`s3_plain_rewritable` للجداول غير القابلة للتغيير وغير المكررة في S3](/ar/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## استخدام عدة أجهزة تخزين كتلية لتخزين البيانات
</div>

<div id="introduction">
  ### المقدمة
</div>

يمكن لمحركات الجداول من عائلة `MergeTree` تخزين البيانات على عدة أجهزة تخزين كتلية. على سبيل المثال، قد يكون ذلك مفيدًا عندما تُقسَّم بيانات جدول معيّن ضمنيًا إلى بيانات &quot;ساخنة&quot; وأخرى &quot;باردة&quot;. تُطلَب أحدث البيانات بانتظام، لكنها لا تحتاج إلا إلى مساحة صغيرة. في المقابل، نادرًا ما تُطلَب الكمية الكبيرة من البيانات التاريخية. وإذا كانت عدة أقراص متاحة، فقد توضع البيانات &quot;الساخنة&quot; على أقراص سريعة (مثل أقراص NVMe SSD أو في الذاكرة)، بينما توضع البيانات &quot;الباردة&quot; على أقراص أبطأ نسبيًا (مثل HDD).

ينطبق هذا على جميع أنواع الأقراص، بما في ذلك S3 وأقراص التخزين الكائني الأخرى. على سبيل المثال، يمكنك توزيع البيانات عبر عدة حاويات S3 ضمن وحدة تخزين واحدة، أو إنشاء سياسات تخزين متدرجة تنقل البيانات من الأقراص المحلية إلى S3. راجع [استخدام أقراص S3 مع وحدات تخزين متعددة](#s3-multiple-volumes) للاطلاع على التفاصيل.

يمثّل جزء البيانات أصغر وحدة قابلة للنقل في الجداول التي تستخدم محركات `MergeTree`. وتُخزَّن البيانات التابعة لجزء واحد على قرص واحد. ويمكن نقل أجزاء البيانات بين الأقراص في الخلفية (وفقًا لإعدادات المستخدم) وكذلك باستخدام استعلامات [ALTER](/ar/sql-reference/statements/alter/partition).

<div id="terms">
  ### المصطلحات
</div>

* القرص — جهاز تخزين كتلي مربوط بنظام الملفات.
* القرص الافتراضي — القرص الذي يخزّن المسار المحدد في إعداد الخادم [path](/ar/operations/server-configuration-parameters/settings.md/#path).
* وحدة التخزين — مجموعة مرتبة من الأقراص المتكافئة (مشابهة لـ [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
* سياسة التخزين — مجموعة من وحدات التخزين والقواعد الخاصة بنقل البيانات بينها.

يمكن العثور على أسماء الكيانات الموصوفة في جداول النظام [system.storage&#95;policies](/ar/operations/system-tables/storage_policies) و[system.disks](/ar/operations/system-tables/disks). لتطبيق إحدى سياسات التخزين المُعَدّة على جدول، استخدم الإعداد `storage_policy` في جداول عائلة المحرك `MergeTree`.

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### التهيئة
</div>

يجب تعريف الأقراص ووحدات التخزين وسياسات التخزين داخل الوسم `<storage_configuration>`، وذلك في ملف ضمن الدليل `config.d`.

:::tip
يمكن أيضًا تعريف الأقراص في قسم `SETTINGS` ضمن استعلام. ويكون هذا مفيدًا
للتحليل المخصص لإرفاق قرص مؤقتًا يكون مستضافًا، على سبيل المثال، على URL.
راجع [التخزين الديناميكي](/ar/operations/storing-data#dynamic-configuration) لمزيد من التفاصيل.
:::

بنية التهيئة:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

الوسوم:

* `<disk_name_N>` — اسم القرص. يجب أن تكون الأسماء مختلفة لكل الأقراص.
* `path` — المسار الذي سيخّن فيه الخادم البيانات (مجلدا `data` و`shadow`)، ويجب أن ينتهي بـ &#39;/&#39;.
* `keep_free_space_bytes` — مقدار المساحة الحرة على القرص التي يجب حجزها.

ترتيب تعريف القرص غير مهم.

ترميز إعدادات سياسات التخزين:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

الوسوم:

* `policy_name_N` — اسم السياسة. يجب أن تكون أسماء السياسات فريدة.
* `volume_name_N` — اسم وحدة التخزين. يجب أن تكون أسماء وحدات التخزين فريدة.
* `disk` — قرص ضمن وحدة تخزين.
* `max_data_part_size_bytes` — الحد الأقصى لحجم الجزء الذي يمكن تخزينه على أيٍّ من أقراص وحدة التخزين. إذا كان الحجم التقديري لجزء مدمج أكبر من `max_data_part_size_bytes`، فسيُكتب هذا الجزء في وحدة التخزين التالية. تتيح هذه الميزة أساسًا الاحتفاظ بالأجزاء الجديدة/الصغيرة على وحدة تخزين سريعة (SSD) ونقلها إلى وحدة تخزين باردة (HDD) عندما يصبح حجمها كبيرًا. لا تستخدم هذا الإعداد إذا كانت سياستك تحتوي على وحدة تخزين واحدة فقط.
* `move_factor` — عندما تنخفض المساحة المتاحة إلى ما دون هذا العامل، تبدأ البيانات تلقائيًا بالانتقال إلى وحدة التخزين التالية إن وُجدت (القيمة الافتراضية 0.1). يرتّب ClickHouse الأجزاء الموجودة حسب الحجم من الأكبر إلى الأصغر (ترتيبًا تنازليًا) ويختار الأجزاء التي يكون مجموع أحجامها كافيًا لتحقيق شرط `move_factor`. وإذا لم يكن الحجم الإجمالي لجميع الأجزاء كافيًا، فستنقل جميع الأجزاء.
* `perform_ttl_move_on_insert` — يعطّل `TTL move` عند INSERT لجزء البيانات. افتراضيًا (إذا كان مُمكّنًا)، إذا أدرجنا جزء بيانات انتهت صلاحيته بالفعل وفق قاعدة `TTL move`، فإنه ينتقل فورًا إلى وحدة التخزين/القرص المحدد في قاعدة النقل. وقد يؤدي ذلك إلى إبطاء عملية `insert` بشكل كبير إذا كانت وحدة التخزين/القرص الوجهة بطيئة (مثل S3). وإذا كان هذا الإعداد معطّلًا، فسيُكتب جزء البيانات المنتهية صلاحيته بالفعل إلى وحدة التخزين الافتراضية ثم يُنقل مباشرة بعد ذلك إلى وحدة تخزين TTL.
* `load_balancing` - سياسة موازنة الأقراص، `round_robin` أو `least_used`.
* `least_used_ttl_ms` - اضبط المهلة الزمنية (بالملي ثانية) لتحديث المساحة المتاحة على جميع الأقراص (`0` - تحديث دائمًا، `-1` - عدم التحديث مطلقًا، والقيمة الافتراضية هي `60000`). لاحظ أنه إذا كان ClickHouse هو الجهة الوحيدة التي تستخدم القرص، ولم يكن القرص خاضعًا لتوسيع/تقليص `filesystem` أثناء التشغيل، فيمكنك استخدام `-1`. أما في جميع الحالات الأخرى فلا يُنصح بذلك، لأنه سيؤدي في النهاية إلى توزيع غير صحيح للمساحة.
* `prefer_not_to_merge` — يجب ألا تستخدم هذا الإعداد. فهو يعطّل دمج أجزاء البيانات على وحدة التخزين هذه (وهذا ضار ويؤدي إلى تدهور الأداء). عند تمكين هذا الإعداد (لا تفعل ذلك)، لا يُسمح بدمج البيانات على وحدة التخزين هذه (وهذا أمر سيئ). يتيح ذلك (لكنك لا تحتاج إليه) التحكم (إذا كنت تريد التحكم في شيء هنا، فأنت ترتكب خطأ) في كيفية تعامل ClickHouse مع الأقراص البطيئة (لكن ClickHouse يعرف الأفضل، لذا يُرجى عدم استخدام هذا الإعداد).
* `volume_priority` — يحدد الأولوية (الترتيب) التي تُملأ بها وحدات التخزين. وتعني القيمة الأقل أولوية أعلى. يجب أن تكون قيم المعلَمة أعدادًا طبيعية، وأن تغطي مجتمعة النطاق من 1 إلى N (بحيث تمثل N أدنى أولوية) من دون تخطي أي أرقام.
  * إذا كانت *جميع* وحدات التخزين موسومة، فستُعطى الأولوية لها بالترتيب المحدد.
  * إذا كانت *بعض* وحدات التخزين فقط موسومة، فستكون الوحدات غير الموسومة ذات الأولوية الأدنى، وتُرتَّب حسب ترتيب تعريفها في `config`.
  * إذا لم تكن *أي* من وحدات التخزين موسومة، فستُضبط أولويتها وفقًا لترتيب تعريفها في التهيئة.
  * لا يمكن أن تكون لوحدتي تخزين قيمة الأولوية نفسها.

أمثلة على التهيئة:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

في هذا المثال، تطبّق السياسة `hdd_in_order` أسلوب [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling). لذلك، لا تحدد هذه السياسة سوى وحدة تخزين واحدة (`single`)، وتُخزَّن أجزاء البيانات على جميع أقراصها بالتناوب. وقد تكون هذه السياسة مفيدة جدًا إذا كانت هناك عدة أقراص متشابهة مربوطة بالنظام، ولكن RAID غير مُعدّ. ضع في اعتبارك أن كل قرص على حدة ليس موثوقًا، وقد ترغب في تعويض ذلك بمعامل نسخ متماثل قدره 3 أو أكثر.

إذا كانت هناك أنواع مختلفة من الأقراص متاحة في النظام، فيمكن استخدام السياسة `moving_from_ssd_to_hdd` بدلًا من ذلك. تتكوّن وحدة التخزين `hot` من قرص SSD (`fast_ssd`)، ويبلغ الحد الأقصى لحجم جزء البيانات الذي يمكن تخزينه على وحدة التخزين هذه 1GB. وستُخزَّن جميع الأجزاء التي يزيد حجمها على 1GB مباشرةً على وحدة التخزين `cold`، التي تحتوي على قرص HDD باسم `disk1`.
كذلك، ما إن تتجاوز نسبة امتلاء القرص `fast_ssd`‏ 80% حتى تُنقَل البيانات إلى `disk1` بواسطة عملية تعمل في الخلفية.

ويُعد ترتيب سرد وحدات التخزين داخل سياسة التخزين مهمًا إذا كانت واحدة على الأقل من وحدات التخزين المدرجة لا تحتوي على المعامل `volume_priority` بشكل صريح.
وبمجرد أن تمتلئ وحدة تخزين أكثر من اللازم، تُنقَل البيانات إلى التي تليها. كما أن ترتيب سرد الأقراص مهم أيضًا لأن البيانات تُخزَّن عليها بالتناوب.

عند إنشاء جدول، يمكن تطبيق إحدى سياسات التخزين المُعدّة عليه:

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

تعني سياسة التخزين `default` استخدام وحدة تخزين واحدة فقط، تتكوّن من قرص واحد فقط محدَّد في `<path>`.
يمكنك تغيير سياسة التخزين بعد إنشاء الجدول باستخدام الاستعلام [ALTER TABLE ... MODIFY SETTING]، ويجب أن تتضمن السياسة الجديدة جميع الأقراص ووحدات التخزين القديمة نفسها وبالأسماء نفسها.

يمكن تغيير عدد الخيوط التي تنفّذ عمليات نقل أجزاء البيانات في الخلفية عبر الإعداد [background&#95;move&#95;pool&#95;size](/ar/operations/server-configuration-parameters/settings.md/#background_move_pool_size).

<div id="details">
  ### التفاصيل
</div>

في حالة جداول `MergeTree`، تصل البيانات إلى القرص بطرق مختلفة:

* نتيجة لعملية إدراج (استعلام `INSERT`).
* أثناء عمليات الدمج في الخلفية و[عمليات mutation](/ar/sql-reference/statements/alter#mutations).
* عند التنزيل من نسخة متماثلة أخرى.
* نتيجة لتجميد القسم [ALTER TABLE ... FREEZE PARTITION](/ar/sql-reference/statements/alter/partition#freeze-partition).

في جميع هذه الحالات، باستثناء عمليات mutation وتجميد القسم، يُخزَّن الجزء على وحدة تخزين وقرص وفقًا لسياسة التخزين المحددة:

1. يُختار أول وحدة تخزين (بحسب ترتيب تعريفها) تتوفر فيها مساحة قرص كافية لتخزين الجزء (`unreserved_space > current_part_size`) وتسمح بتخزين أجزاء بهذا الحجم (`max_data_part_size_bytes > current_part_size`).
2. ضمن وحدة التخزين هذه، يُختار القرص الذي يلي القرص المستخدم لتخزين كتلة البيانات السابقة، والذي تكون فيه المساحة الحرة أكبر من حجم الجزء (`unreserved_space - keep_free_space_bytes > current_part_size`).

داخليًا، تستخدم عمليات mutation وتجميد القسم [الروابط الصلبة](https://en.wikipedia.org/wiki/Hard_link). لا يتم دعم الروابط الصلبة بين الأقراص المختلفة، لذلك في مثل هذه الحالات تُخزَّن الأجزاء الناتجة على الأقراص نفسها التي توجد عليها الأجزاء الأصلية.

في الخلفية، تُنقل الأجزاء بين وحدات التخزين استنادًا إلى مقدار المساحة الحرة (المعلمة `move_factor`) وفقًا لترتيب تعريف وحدات التخزين في ملف الإعدادات.
ولا تُنقل البيانات أبدًا من الأخيرة إلى الأولى. يمكن استخدام جداول النظام [system.part&#95;log](/ar/operations/system-tables/part_log) (الحقل `type = MOVE_PART`) و[system.parts](/ar/operations/system-tables/parts.md) (الحقلان `path` و`disk`) لمراقبة عمليات النقل في الخلفية. كذلك، يمكن العثور على معلومات مفصلة في سجلات الخادم.

يمكن للمستخدم فرض نقل جزء أو قسم من وحدة تخزين إلى أخرى باستخدام الاستعلام [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/ar/sql-reference/statements/alter/partition)، مع أخذ جميع القيود الخاصة بعمليات الخلفية في الحسبان. يبدأ الاستعلام عملية النقل بنفسه ولا ينتظر اكتمال عمليات الخلفية. سيتلقى المستخدم رسالة خطأ إذا لم تكن هناك مساحة حرة كافية أو إذا لم يتم استيفاء أي من الشروط المطلوبة.

لا يتداخل نقل البيانات مع نسخ البيانات المتماثل. ولذلك، يمكن تحديد سياسات تخزين مختلفة للجدول نفسه على نسخ متماثلة مختلفة.

بعد اكتمال عمليات الدمج في الخلفية وعمليات mutation، لا تُزال الأجزاء القديمة إلا بعد مرور مدة زمنية معينة (`old_parts_lifetime`).
وخلال هذه المدة، لا تُنقل إلى وحدات تخزين أو أقراص أخرى. لذلك، وحتى تتم إزالة الأجزاء نهائيًا، فإنها تظل محسوبة عند تقييم مساحة القرص المشغولة.

يمكن للمستخدم توزيع الأجزاء الكبيرة الجديدة على أقراص مختلفة ضمن وحدة تخزين من نوع [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) بطريقة متوازنة باستخدام الإعداد [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/ar/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod).

<div id="table_engine-mergetree-s3">
  ## استخدام التخزين الخارجي لتخزين البيانات
</div>

يمكن لمحركات الجداول من عائلة [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree.md) تخزين البيانات على `S3` و`AzureBlobStorage` و`HDFS` باستخدام قرص من النوع `s3` أو `azure_blob_storage` أو `hdfs` على التوالي. راجع [تهيئة خيارات التخزين الخارجي](/ar/operations/storing-data.md/#configuring-external-storage) لمزيد من التفاصيل.

مثال على استخدام [S3](https://aws.amazon.com/s3/) كتخزين خارجي باستخدام قرص من النوع `s3`.

ترميز التهيئة:

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

انظر أيضًا [تهيئة خيارات التخزين الخارجي](/ar/operations/storing-data.md/#configuring-external-storage).

<div id="s3-multiple-volumes">
  ### استخدام أقراص S3 مع وحدات تخزين متعددة
</div>

يمكن استخدام أقراص S3 (وأقراص التخزين الكائني الأخرى) ضمن سياسات التخزين متعددة الأقراص ومتعددة وحدات التخزين بالطريقة نفسها المستخدمة مع الأقراص المحلية. يتيح لك ذلك توزيع البيانات على عدة حاويات S3 داخل وحدة تخزين واحدة (على نمط JBOD)، أو إعداد سياسات تخزين على طبقات باستخدام وحدات تخزين S3.

على سبيل المثال، لتوزيع البيانات على حاويتَي S3 بأسلوب round-robin:

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

يمكنك أيضًا الجمع بين وحدات التخزين المحلية ووحدات تخزين S3 ضمن سياسة متعددة المستويات، مثل نقل البيانات من Local SSD إلى S3 مع تقادمها:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
عند استخدام `use_environment_credentials` لمصادقة S3، تتم مشاركة بيانات الاعتماد المستمدة من البيئة (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) بين جميع أقراص S3. ولا يمكن استخدام بيانات اعتماد بيئية مختلفة لأقراص مختلفة. إذا كنت بحاجة إلى بيانات اعتماد مختلفة لكل قرص S3، فاستخدم بدلًا من ذلك إعدادات `access_key_id` و`secret_access_key` الصريحة لكل قرص.
:::

يمكن إعداد جداول MergeTree غير المُكرّرة لسيناريو كاتب واحد وعدة قرّاء على تخزين مشترك. ويتحقق ذلك عبر التحديث التلقائي لقائمة الأجزاء، الذي يمكن تفعيله على القرّاء. لاحظ أن هذا يتطلب بيانات وصفية مشتركة لنظام الملفات عبر النسخ المتماثلة (أو `table_disk = true` مع قرص محلي خاص بالجدول). راجع [refresh&#95;parts&#95;interval and table&#95;disk](/ar/operations/storing-data.md/#refresh-parts-interval-and-table-disk).

:::note إعدادات cache
تستخدم إصدارات ClickHouse من 22.3 إلى 22.7 إعدادات cache مختلفة؛ راجع [using local cache](/ar/operations/storing-data.md/#using-local-cache) إذا كنت تستخدم أحد هذه الإصدارات.
:::

<div id="virtual-columns">
  ## الأعمدة الافتراضية
</div>

* `_part` — اسم الجزء.
* `_part_index` — الفهرس التسلسلي للجزء في نتيجة الاستعلام.
* `_part_starting_offset` — صف البداية التراكمي للجزء في نتيجة الاستعلام.
* `_part_offset` — رقم الصف داخل الجزء.
* `_part_granule_offset` — رقم الحبيبة داخل الجزء.
* `_partition_id` — اسم القسم.
* `_part_uuid` — المعرّف الفريد للجزء (إذا كان إعداد MergeTree ‏`assign_part_uuids` مُمكّنًا).
* `_part_data_version` — إصدار بيانات الجزء (إما الحد الأدنى لرقم block أو إصدار mutation).
* `_partition_value` — القيم (tuple) لتعبير `partition by`.
* `_sample_factor` — عامل العيّنة (من الاستعلام).
* `_block_number` — الرقم الأصلي للـ block الخاص بالصف، والذي أُسنِد عند insert، ويُحفَظ عبر merges عندما يكون الإعداد `enable_block_number_column` مُمكّنًا.
* `_block_offset` — الرقم الأصلي للصف داخل block، والذي أُسنِد عند insert، ويُحفَظ عبر merges عندما يكون الإعداد `enable_block_offset_column` مُمكّنًا.
* `_disk_name` — اسم قرص المستخدم للتخزين.

<div id="column-statistics">
  ## إحصاءات الأعمدة
</div>

<CloudNotSupportedBadge />

يرِد تعريف الإحصاءات في قسم الأعمدة ضمن استعلام `CREATE` للجداول من عائلة `*MergeTree*`:

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

يمكننا أيضًا تعديل الإحصاءات باستخدام عبارات `ALTER`:

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

تجمع هذه الإحصاءات الخفيفة معلومات مجمّعة عن توزيع القيم في الأعمدة. وتُخزَّن الإحصاءات في كل جزء، ويجري تحديثها مع كل عملية إدراج.
ولا يمكن استخدامها لتحسين prewhere إلا عند تمكين `set use_statistics = 1`.

<div id="part-pruning-with-statistics">
  #### استبعاد الأجزاء باستخدام الإحصاءات
</div>

عند تمكين `use_statistics_for_part_pruning`، يمكن استخدام الإحصاءات لاستبعاد الأجزاء.
حاليًا، لا تدعم استبعاد الأجزاء إلا إحصاءات `MinMax` و`Basic`. وعند تعريف هذا النوع من الإحصاءات على عمود، يتتبّع ClickHouse القيمتَين الصغرى والكبرى لذلك العمود في كل جزء.
ويتيح استبعاد الأجزاء تخطي قراءة أجزاء البيانات بأكملها عندما يتعذر أن يطابق شرط تصفية الاستعلام أي صفوف في ذلك الجزء.

**مثال:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### الأنواع المتاحة من إحصاءات الأعمدة
</div>

* `Basic`

  حزمة مضغوطة من الملخصات أحادية القيمة المستمدة من عمود. وبحسب نوع العمود، تُملأ العناصر التالية:

  * لأي عمود تُمثَّل قيمه بأرقام (الأعداد الصحيحة، والأعداد العائمة، و`Decimal*`، و`Date*`، و`DateTime*`، و`Enum*`، و`IPv4`، ...): القيمة الدنيا والقيمة القصوى، ما يتيح تقدير selectivity لعوامل تصفية النطاق ويمكّن part pruning;
  * لأعمدة `String` و`FixedString`: إجمالي طول القيم غير `NULL` بالبايت (الذي يمكن اشتقاق متوسط طول السلسلة منه);
  * لأعمدة `Nullable` و`LowCardinality(Nullable)`: عدد قيم `NULL`، الذي يستخدمه المُحسِّن لاستبعاد صفوف `NULL` من تقديرات selectivity.

    يمكن لإحصائية `Basic` واحدة أن تملأ عدة عناصر من هذه العناصر في الوقت نفسه — فعلى سبيل المثال، في عمود `Nullable(UInt32)` تتتبّع كلاً من الحدين الأدنى/الأقصى العدديين وعدد القيم `NULL`. وبالمقارنة مع `MinMax`، تعمل `Basic` أيضًا مع أعمدة `String` / `FixedString`، ويمكن التصريح بها على wrappers من نوع `Nullable` لأنواع مثل `UUID` أو `IPv6` فقط لتتبّع عدد القيم `NULL`.

    البنية: `basic`

* `MinMax`

  القيمة الدنيا والقيمة القصوى للعمود، ما يتيح تقدير selectivity لعوامل تصفية النطاق على الأعمدة الرقمية.

  البنية: `minmax`

* `TDigest`

:::warning
إحصاءات النوع `tdigest` مرتفعة التكلفة عند الإنشاء، وقد تؤدي إلى إبطاء إدخال البيانات.
:::

ملخصات [TDigest](https://github.com/tdunning/t-digest) التي تتيح حساب القيم المئينية التقريبية (مثل المئين التسعين) للأعمدة الرقمية.

البنية: `tdigest`

* `Uniq`

  ملخصات [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) التي توفر تقديرًا لعدد القيم المميزة التي يحتوي عليها العمود.

  البنية: `uniq`

* `CountMin`

:::warning
إحصاءات النوع `countmin` مرتفعة التكلفة عند الإنشاء، وقد تؤدي إلى إبطاء إدخال البيانات.
:::

ملخصات [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) التي توفر عددًا تقريبيًا لتكرار كل قيمة في العمود.

البنية: `countmin`

<div id="supported-data-types">
  ### أنواع البيانات المدعومة
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | IPv4 | String or FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

تقبل جميع ما سبق أيضًا مغلّفات `Nullable` و`LowCardinality(Nullable)` للأنواع المدرجة. ويمكن أيضًا تعريف `Basic` على مغلّفات `Nullable` لأنواع مثل `UUID` أو `IPv6` فقط لتتبّع عدد قيم NULL.

<div id="supported-operations">
  ### العمليات المدعومة
</div>

|          | عوامل تصفية المساواة (==) | عوامل تصفية النطاق (`>, >=, <, <=`) |
| -------- | ------------------------- | ----------------------------------- |
| Basic    | ✗                         | ✔ (للأعمدة الرقمية فقط)             |
| CountMin | ✔                         | ✗                                   |
| MinMax   | ✗                         | ✔ (للأعمدة الرقمية فقط)             |
| TDigest  | ✗                         | ✔ (للأعمدة الرقمية فقط)             |
| Uniq     | ✔                         | ✗                                   |

بالنسبة إلى `Basic` على أعمدة `String` / `FixedString`، لا تسجّل هذه الإحصائية سوى
إجمالي طول البايتات غير `NULL` (ويُستخدم ذلك لتقدير متوسط طول السلسلة النصية) وعدد القيم `NULL`؛
ولا تُستخدم كأساس لعوامل تصفية النطاق أو استبعاد الأجزاء.

<div id="column-level-settings">
  ## الإعدادات على مستوى العمود
</div>

يمكن تجاوز بعض إعدادات MergeTree على مستوى العمود:

* `max_compress_block_size` — الحد الأقصى لحجم كتل البيانات غير المضغوطة قبل ضغطها عند الكتابة إلى جدول.
* `min_compress_block_size` — الحد الأدنى لحجم كتل البيانات غير المضغوطة المطلوب لضغطها عند كتابة العلامة التالية.

مثال:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

يمكن تعديل إعدادات العمود أو إزالتها باستخدام [ALTER MODIFY COLUMN](/ar/sql-reference/statements/alter/column.md)، على سبيل المثال:

* إزالة `SETTINGS` من تعريف العمود:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* عدِّل إعدادًا:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* يعيد تعيين إعداد واحد أو أكثر، كما يزيل أيضًا تعريف الإعداد من تعبير العمود في استعلام CREATE الخاص بالجدول.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```