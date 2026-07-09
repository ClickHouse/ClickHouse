---
description: 'توثيق لنوع البيانات QBit في ClickHouse، الذي يتيح تكميمًا دقيقًا للبحث التقريبي في المتجهات'
keywords: ['qbit', 'نوع البيانات']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'نوع البيانات QBit'
doc_type: 'مرجع'
---

يعيد نوع البيانات `QBit` تنظيم تخزين المتجهات لتسريع عمليات البحث التقريبي. فبدلًا من تخزين عناصر كل متجه معًا، يجمع مواضع البتات نفسها عبر جميع المتجهات.
ويتيح ذلك تخزين المتجهات بالدقة الكاملة، مع تمكينك من اختيار مستوى التكميم الدقيق وقت البحث: اقرأ عددًا أقل من البتات لتقليل عمليات الإدخال/الإخراج وتسريع العمليات الحسابية، أو عددًا أكبر من البتات للحصول على دقة أعلى. وهكذا تستفيد من مزايا السرعة الناتجة عن تقليل نقل البيانات والحوسبة بفضل التكميم، مع بقاء جميع البيانات الأصلية متاحة عند الحاجة.

للإعلان عن عمود من النوع `QBit`، استخدم الصياغة التالية:

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – نوع كل عنصر في المتجه. الأنواع المسموح بها هي `Int8` و`BFloat16` و`Float32` و`Float64`
* `dimension` – عدد العناصر في كل متجه
* `stride` – اختياري. عدد الأبعاد المخزَّنة معًا ضمن مجموعة واحدة من التدفقات. وعند عدم تحديده، تكون قيمته الافتراضية `dimension` (أي مجموعة واحدة). وعند تحديده، يجب أن تكون `dimension` من مضاعفات `stride`، وإذا كانت `stride` أصغر من `dimension`، فيجب أن تكون `stride` من مضاعفات 8. راجع [Strides](#strides).

<div id="creating-qbit">
  ## إنشاء QBit
</div>

استخدام النوع `QBit` في تعريف عمود في الجدول:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## تحويل المصفوفات إلى QBit
</div>

تُحوَّل المصفوفات إلى `QBit` عندما يطابق طولها البُعد في `QBit`. ولا يشترط أن يطابق نوع عناصر المصفوفة نوع عناصر `QBit`. إذ يُحوَّل إليه تلقائيًا أي نوع عناصر رقمي. ويتيح لك ذلك نقل عمود موجود من embeddings مباشرةً إلى عمود `QBit`:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

يمكن أيضًا إجراء التحويل صراحةً باستخدام `CAST`، على سبيل المثال `CAST(embedding AS QBit(Float32, 8))`.

<div id="converting-qbit-to-arrays">
  ## تحويل QBit إلى مصفوفات
</div>

تعيد عملية التحويل العكسية إنشاء المتجه الأصلي انطلاقًا من التمثيل المنقول للبتات، لذا فإن تحويل `QBit` إلى `Array` يعيد القيم المخزّنة. وهذا هو عكس [تحويل المصفوفات إلى `QBit`](#converting-arrays-to-qbit):

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

تستخدم المصفوفة المُعاد تكوينها نوع العنصر في `QBit`، ثم تُحوَّل عناصرها إلى نوع عنصر المصفوفة المطلوب. لذلك، ينجح أيضًا تحويل `CAST` الذي يغيّر نوع العنصر، مثل التحويل من `QBit(Float32, N)` إلى `Array(Float64)`.

تكون دورة التحويل `Array` -&gt; `QBit` -&gt; `Array` بلا فقدان للبيانات مع `Int8` و`Float32` و`Float64`. أما في حالة `BFloat16`، فهي تطابق التحويل المباشر إلى `BFloat16` — والفقد الوحيد في `precision` هو الفقد الملازم لـ `BFloat16` نفسه.

عندما لا يكون `dimension` من مضاعفات 8، تُحذف عناصر `padding` اللاحقة الموجودة في التمثيل الداخلي، بحيث تحتوي النتيجة دائمًا على `dimension` عنصرًا بالضبط.

<div id="qbit-subcolumns">
  ## الأعمدة الفرعية في QBit
</div>

يطبّق `QBit` نمط وصول إلى الأعمدة الفرعية يتيح لك الوصول إلى مستويات البت الفردية للمتجهات المخزّنة. ويمكن الوصول إلى كل موضع بت باستخدام الصيغة `.N`، حيث يمثّل `N` موضع البت:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

يعتمد عدد الأعمدة الفرعية المتاحة على نوع العنصر (وعند استخدام Strides، يعتمد أيضًا على عدد مجموعات stride — راجع [Strides](#strides)):

* `Int8`: 8 أعمدة فرعية لكل مجموعة stride (1-8)
* `BFloat16`: 16 عمودًا فرعيًا لكل مجموعة stride (1-16)
* `Float32`: 32 عمودًا فرعيًا لكل مجموعة stride (1-32)
* `Float64`: 64 عمودًا فرعيًا لكل مجموعة stride (1-64)

<div id="strides">
  ## قيم stride
</div>

افتراضيًا، يخزّن `QBit` كل مستوى بت في `single stream` واحد يمتد عبر جميع أبعاد `dimension`، لذا فإن أي عملية بحث تقرأ دائمًا مستويات البت كاملة عبر المتجه بأكمله. يقسّم المعامل الاختياري `stride` أبعاد `dimension` إلى `dimension / stride` مجموعات متجاورة، ويخزّن مستويات البت الخاصة بكل مجموعة في `تدفقات` منفصلة. يتيح ذلك لعملية بحث تقتصر على أول `D` بُعدًا فقط (بحيث تكون `D` من مضاعفات `stride`) أن تقرأ فقط `تدفقات` الخاصة بالمجموعات التي تغطي تلك الأبعاد — وهذا مفيد مع [Matryoshka embeddings](https://arxiv.org/abs/2205.13147)، حيث تكوّن الأبعاد الأولى تضمينًا منخفض الأبعاد قابلًا للاستخدام.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

هنا تُقسَّم الأبعاد الـ4096 إلى 4 مجموعات من 1024. وتتبع الأعمدة الفرعية ترتيبًا يبدأ بالمجموعات: مع `BFloat16` (16 مستوى بت)، تمثل `vec.1` … `vec.16` مستويات البت الستة عشر لمجموعة الـstride الأولى (الأبعاد 1–1024)، وتنتمي `vec.17` … `vec.32` إلى المجموعة الثانية (الأبعاد 1025–2048)، وهكذا. وبوجه عام، يقرأ `vec.N` مستوى البت `(N-1) % element_size` من مجموعة الـstride رقم `(N-1) / element_size`.

لتنفيذ بحث بأبعاد مخفّضة، مرِّر عدد الأبعاد المطلوب قراءتها باعتباره الوسيطة الرابعة لدوال المسافة المنقولة (انظر أدناه). ويجب أن يحتوي المتجه المرجعي على هذا العدد من العناصر بالضبط، كما يجب أن تكون القيمة من مضاعفات `stride`.

<div id="vector-search-functions">
  ## دوال البحث المتجهي
</div>

هذه هي دوال المسافة الخاصة بالبحث عن التشابه بين المتجهات التي تستخدم نوع البيانات `QBit`:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

بالنسبة إلى `QBit` ذي stride، تقبل هذه الدوال وسيطًا رابعًا اختياريًا هو `used_dims` — أي عدد الأبعاد الأولى المطلوب قراءتها — بحيث لا تُقرأ إلا مجموعات stride التي تغطي تلك الأبعاد:

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```