---
description: 'توثيق أنواع البيانات ذات الفاصلة العائمة في ClickHouse: Float32،
  Float64، وBFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'أنواع Float32 | Float64 | BFloat16'
doc_type: 'مرجع'
---

:::note
إذا كنت بحاجة إلى حسابات دقيقة، وخاصةً إذا كنت تعمل على بيانات مالية أو بيانات أعمال تتطلب دقة عالية، فمن الأفضل استخدام [Decimal](../data-types/decimal.md) بدلًا من ذلك.

قد تؤدي [الأعداد ذات الفاصلة العائمة](https://en.wikipedia.org/wiki/IEEE_754) إلى نتائج غير دقيقة، كما هو موضح أدناه:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

الأنواع المكافئة في ClickHouse وC موضحة أدناه:

* `Float32` — `float`.
* `Float64` — `double`.

لأنواع Float في ClickHouse الأسماء المستعارة التالية:

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

عند إنشاء الجداول، يمكن تحديد المعلمات الرقمية للأعداد ذات الفاصلة العائمة (مثل `FLOAT(12)` و`FLOAT(15, 22)` و`DOUBLE(12)` و`DOUBLE(4, 18)`)، لكن ClickHouse يتجاهلها.

<div id="using-floating-point-numbers">
  ## استخدام الأعداد ذات الفاصلة العائمة
</div>

* قد تسفر العمليات الحسابية على الأعداد ذات الفاصلة العائمة عن خطأ ناتج عن التقريب.

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* تعتمد نتيجة الحساب على طريقة إجرائه (نوع المعالج ومعمارية نظام الحاسوب).
* قد تسفر العمليات الحسابية ذات الفاصلة العائمة عن قيم مثل اللانهاية (`Inf`) و&quot;ليس عددًا&quot; (`NaN`). ينبغي أخذ ذلك في الاعتبار عند معالجة نتائج الحسابات.
* عند تحليل الأعداد ذات الفاصلة العائمة من النص، قد لا تكون النتيجة أقرب عدد يمكن للآلة تمثيله.

<div id="nan-and-inf">
  ## NaN و Inf
</div>

على عكس SQL المعياري، يدعم ClickHouse الفئات التالية من الأعداد ذات الفاصلة العائمة:

* `Inf` – اللانهاية.

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — سالب اللانهاية.

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — ليس عددًا.

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

راجع قواعد ترتيب `NaN` في قسم [عبارة ORDER BY](../../sql-reference/statements/select/order-by.md).

<div id="nan-values-in-set-semantics">
  ## قيم `NaN` في دلالات المجموعات
</div>

يُعرّف معيار IEEE 754 القيمة `NaN` بحيث إن المقارنة السلمية `NaN = NaN` تُرجع `false`.
ويتبع ClickHouse هذه القاعدة مع العامل `=`.

ومع ذلك، فإن `NaN` ليست قيمة واحدة؛ بل هي أي نمط بتات يكون فيه الأسّ مكوّنًا بالكامل من الواحدات وتكون
المانتيسا غير صفرية. ويمكن أن تُنتج العمليات المختلفة ومعماريات CPU المختلفة قيم `NaN`
ذات بتات إشارة مختلفة أو حمولات مانتيسا مختلفة. على سبيل المثال:

* تنتج `0./0.` قيمة `NaN` يكون بت الإشارة فيها 1 على معظم منصات x86.
* تنتج القيمة الحرفية `nan` قيمة `NaN` يكون بت الإشارة فيها 0.
* بعد [طلب سحب #98230](https://github.com/ClickHouse/ClickHouse/pull/98230)، يُرجع مسار AArch64 NEON لـ
  `log` قيمة `NaN` يختلف فيها بت الإشارة عن `log` السلمي في glibc عند المُدخلات السالبة.

تقارن جداول التجزئة في ClickHouse المفاتيح بايتًا ببايت، لذا فإن أنماط البتات المختلفة لـ `NaN` تُجزَّأ إلى
خانات مختلفة وتُعامل على أنها قيم متميزة في العمليات ذات دلالات المجموعات، بما في ذلك
`DISTINCT` و`GROUP BY` و`uniqExact` و`countDistinct` و`JOIN` التكافئي على مفتاح `Float`:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

هذا متوافق مع IEEE 754 (كل `NaN` لا يساوي أي قيمة أخرى، بما في ذلك نفسه)
لكن قد يبدو ذلك مفاجئًا. إذا كنت بحاجة إلى أن تتعامل العمليات ذات دلالات المجموعات مع جميع قيم `NaN` على أنها متساوية،
فوحِّد تمثيلها إلى الصيغة القياسية في الاستعلام:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

ينطبق النهج نفسه على مفاتيح `DISTINCT` و`GROUP BY` و`JOIN`.

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` هو نوع بيانات للأعداد ذات الفاصلة العائمة بعرض 16 بت، مع أسّ بعرض 8 بت، وإشارة، ومانتيسا بعرض 7 بت.
وهو مفيد لتطبيقات تعلّم الآلة والذكاء الاصطناعي.

يدعم ClickHouse التحويل بين `Float32` و`BFloat16`، ويمكن إجراء ذلك باستخدام الدالة [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) أو [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16).

:::note
معظم العمليات الأخرى غير مدعومة.
:::