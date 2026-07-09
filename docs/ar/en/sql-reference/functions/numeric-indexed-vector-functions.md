---
description: 'توثيق NumericIndexedVector ودواله'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'دوال NumericIndexedVector'
doc_type: 'reference'
---

‏NumericIndexedVector هو بنية بيانات مجردة تتضمن متجهًا وتنفّذ عمليات تجميع المتجه والعمليات على مستوى العناصر. ويستخدم أسلوب التخزين Bit-Sliced Index. للاطلاع على الأساس النظري وحالات الاستخدام، راجع الورقة البحثية [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

<div id="bit-sliced-index">
  ## BSI
</div>

في أسلوب التخزين BSI ‏(Bit-Sliced Index)، تُخزَّن البيانات بصيغة [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) ثم تُضغط باستخدام [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap). وتُنفَّذ عمليات التجميع والعمليات على مستوى العنصر مباشرةً على البيانات المضغوطة، مما قد يحسّن كفاءة التخزين والاستعلام بشكل كبير.

يحتوي المتجه على الفهارس والقيم المقابلة لها. وفيما يلي بعض خصائص هذا التركيب البياني وقيوده في وضع تخزين BSI:

* يمكن أن يكون نوع الفهرس أحد `UInt8` أو `UInt16` أو `UInt32`. **ملاحظة:** نظرًا إلى أداء تنفيذ Roaring Bitmap ذي 64 بت، فإن تنسيق BSI لا يدعم `UInt64`/`Int64`.
* يمكن أن يكون نوع القيمة أحد `Int8` أو `Int16` أو `Int32` أو `Int64` أو `UInt8` أو `UInt16` أو `UInt32` أو `UInt64` أو `Float32` أو `Float64`. **ملاحظة:** لا يتم توسيع نوع القيمة تلقائيًا. على سبيل المثال، إذا استخدمت `UInt8` نوعًا للقيمة، فإن أي مجموع يتجاوز سعة `UInt8` سيتسبب في overflow بدلًا من ترقيته إلى نوع أعلى. وبالمثل، فإن العمليات على الأعداد الصحيحة ستنتج نتائج صحيحة (فمثلًا، لن تتحول القسمة تلقائيًا إلى نتيجة فاصلة عائمة). لذلك، من المهم التخطيط لنوع القيمة وتصميمه مسبقًا. وفي السيناريوهات العملية، تُستخدم عادةً الأنواع ذات الفاصلة العائمة (`Float32`/`Float64`).
* لا يمكن تنفيذ العمليات إلا بين متجهين لهما نوع الفهرس نفسه ونوع القيمة نفسه.
* تستخدم طبقة التخزين الأساسية Bit-Sliced Index، حيث يُستخدم bitmap لتخزين الفهارس. ويُستخدم Roaring Bitmap بوصفه تنفيذًا محددًا لـ bitmap. ومن أفضل الممارسات تجميع الفهارس في أقل عدد ممكن من حاويات Roaring Bitmap لتحقيق أقصى استفادة من الضغط وأداء الاستعلام.
* تُحوِّل آلية Bit-Sliced Index القيم إلى تمثيل ثنائي. وبالنسبة إلى الأنواع ذات الفاصلة العائمة، يستخدم التحويل تمثيل الفاصلة الثابتة، مما قد يؤدي إلى فقدان في الدقة. ويمكن ضبط الدقة عبر تخصيص عدد البتات المستخدمة للجزء الكسري، والقيمة الافتراضية هي 24 بت، وهي كافية لمعظم السيناريوهات. يمكنك تخصيص عدد بتات الجزء الصحيح وبتات الجزء الكسري عند إنشاء NumericIndexedVector باستخدام الدالة التجميعية groupNumericIndexedVector مع `-State`.
* توجد ثلاث حالات للفهارس: قيمة غير صفرية، وقيمة صفرية، وغير موجودة. في NumericIndexedVector، لا تُخزَّن إلا القيم غير الصفرية والقيم الصفرية. بالإضافة إلى ذلك، في العمليات على مستوى العنصر بين مثيلين من NumericIndexedVector، ستُعامل قيمة الفهرس غير الموجود على أنها 0. وفي حالة القسمة، تكون النتيجة صفرًا عندما يكون divisor صفرًا.

<div id="create-numeric-indexed-vector-object">
  ## إنشاء كائن numericIndexedVector
</div>

هناك طريقتان لإنشاء هذه البنية: الأولى هي استخدام الدالة التجميعية `groupNumericIndexedVector` مع `-State`.
يمكنك إضافة اللاحقة `-if` لقبول شرط إضافي.
لن تعالج الدالة التجميعية إلا الصفوف التي تستوفي الشرط.
أما الطريقة الأخرى فهي بناؤه من خريطة باستخدام `numericIndexedVectorBuild`.
تتيح الدالة `groupNumericIndexedVectorState` تخصيص عدد البِتّات الصحيحة والكسرية عبر المعاملات، بينما لا يتيح `numericIndexedVectorBuild` ذلك.

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

ينشئ NumericIndexedVector من عمودَي بيانات، ويُرجع مجموع جميع القيم بالنوع `Float64`. وإذا أُضيفت اللاحقة `State`، فإنه يُرجع كائنًا من نوع NumericIndexedVector.

**الصياغة**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**المعلمات**

* `type`: String، اختياري. يحدّد تنسيق التخزين. حاليًا، التنسيق المدعوم الوحيد هو `'BSI'`.
* `integer_bit_num`: `UInt32`، اختياري. يسري هذا المعامل عند استخدام تنسيق التخزين `'BSI'`، ويشير إلى عدد البتات المستخدمة للجزء الصحيح. عندما يكون نوع الفهرس نوعًا صحيحًا، فإن القيمة الافتراضية تساوي عدد البتات المستخدمة لتخزين الفهرس. على سبيل المثال، إذا كان نوع الفهرس هو UInt16، فالقيمة الافتراضية لـ `integer_bit_num` هي 16. وبالنسبة إلى نوعَي الفهرس Float32 وFloat64، تكون القيمة الافتراضية لـ integer&#95;bit&#95;num هي 40، لذا يكون الجزء الصحيح من البيانات الذي يمكن تمثيله ضمن النطاق `[-2^39, 2^39 - 1]`. والنطاق المسموح هو `[0, 64]`.
* `fraction_bit_num`: `UInt32`، اختياري. يسري هذا المعامل عند استخدام تنسيق التخزين `'BSI'`، ويشير إلى عدد البتات المستخدمة للجزء الكسري. عندما يكون نوع القيمة عددًا صحيحًا، تكون القيمة الافتراضية 0؛ وعندما يكون نوع القيمة Float32 أو Float64، تكون القيمة الافتراضية 24. والنطاق الصالح هو `[0, 24]`.
* يوجد أيضًا قيد ينص على أن النطاق الصالح لـ integer&#95;bit&#95;num + fraction&#95;bit&#95;num هو `[0, 64]`.
* `col1`: عمود الفهرس. الأنواع المدعومة: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
* `col2`: عمود القيمة. الأنواع المدعومة: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**القيمة المعادة**

قيمة `Float64` تمثّل مجموع جميع القيم.

**مثال**

بيانات الاختبار:

```text
UserID  PlayTime
1       10
2       20
3       30
```

الاستعلام &amp; النتيجة:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
الوثائق التالية مُولَّدة من جدول النظام `system.functions`.
:::

{/* 
  تُستخدم الوسوم أدناه لإنشاء الوثائق من جداول النظام، ويجب عدم إزالتها.
  لمزيد من التفاصيل، راجع https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }