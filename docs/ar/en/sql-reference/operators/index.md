---
description: 'توثيق المعاملات'
sidebar_label: 'المعاملات'
sidebar_position: 38
slug: /sql-reference/operators/
title: 'المعاملات'
doc_type: 'reference'
---

يحوّل ClickHouse المعاملات إلى الدوال المقابلة لها في مرحلة تحليل الاستعلام، وفقًا لأولويتها وأسبقية التنفيذ والترابط.

<div id="access-operators">
  ## معاملات الوصول
</div>

`a[N]` – الوصول إلى عنصر من عناصر المصفوفة. الدالة `arrayElement(a, N)`.

`a.N` – الوصول إلى عنصر من عناصر Tuple. الدالة `tupleElement(a, N)`.

<div id="numeric-negation-operator">
  ## معامل النفي العددي
</div>

`-a` – الدالة `negate (a)`.

أما لنفي Tuple: [tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate).

<div id="multiplication-and-division-operators">
  ## معاملات الضرب والقسمة
</div>

`a * b` – الدالة `multiply(a, b)`.

لضرب Tuple في عدد: [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber)، ولحساب الضرب القياسي: [dotProduct](/ar/sql-reference/functions/array-functions#arrayDotProduct).

`a / b` – الدالة `divide(a, b)`.

لقسمة Tuple على عدد: [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber).

`a % b` – الدالة `modulo(a, b)`.

<div id="addition-and-subtraction-operators">
  ## معاملات الجمع والطرح
</div>

`a + b` – الدالة `plus(a, b)`.

لجمع قيم `Tuple`: [tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus).

`a - b` – الدالة `minus(a, b)`.

لطرح قيم `Tuple`: [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus).

<div id="comparison-operators">
  ## عوامل المقارنة
</div>

<div id="equals-function">
  ### دالة equals
</div>

`a = b` – هي الدالة `equals(a, b)`.

`a == b` – هي الدالة `equals(a, b)`.

<div id="notequals-function">
  ### دالة notEquals
</div>

`a != b` – دالة `notEquals(a, b)`.

`a <> b` – دالة `notEquals(a, b)`.

<div id="lessorequals-function">
  ### الدالة lessOrEquals
</div>

`a <= b` – الدالة `lessOrEquals(a, b)`.

<div id="greaterorequals-function">
  ### الدالة greaterOrEquals
</div>

`a >= b` – هي الدالة `greaterOrEquals(a, b)`.

<div id="less-function">
  ### دالة less
</div>

`a < b` – الدالة `less(a, b)`.

<div id="greater-function">
  ### دالة greater
</div>

`a > b` – الدالة `greater(a, b)`.

<div id="like-function">
  ### دالة like
</div>

`a LIKE b` – الدالة `like(a, b)`.

<div id="notlike-function">
  ### دالة notLike
</div>

`a NOT LIKE b` – وهي الدالة `notLike(a, b)`.

<div id="ilike-function">
  ### دالة ilike
</div>

`a ILIKE b` – هي الدالة `ilike(a, b)`.

<div id="between-function">
  ### الدالة BETWEEN
</div>

`a BETWEEN b AND c` – تعادل `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – تعادل `a < b OR a > c`.

<div id="is-not-distinct-from">
  ### معامل `is not distinct from` (`<=>`)
</div>

:::note
اعتبارًا من الإصدار 25.10، يمكنك استخدام `<=>` بالطريقة نفسها التي تستخدم بها أي معامل آخر.
قبل الإصدار 25.10، كان لا يمكن استخدامه إلا في تعبيرات JOIN، على سبيل المثال:

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

المعامل `<=>` هو معامل المساواة الآمن مع `NULL`، وهو مكافئ للتعبير `IS NOT DISTINCT FROM`.
يعمل مثل معامل المساواة العادي (`=`)، لكنه يتعامل مع قيم `NULL` على أنها قابلة للمقارنة.
تُعدّ قيمتا `NULL` متساويتين، وعند مقارنة `NULL` بأي قيمة أخرى غير `NULL` تكون النتيجة 0 (`false`) بدلًا من `NULL`.

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

<div id="operators-for-working-with-strings">
  ## المعاملات للتعامل مع السلاسل النصية
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` - الدالة `overlay(string, replacement, offset)`.
* `OVERLAY(string PLACING replacement FROM offset FOR length)` - الدالة `overlay(string, replacement, offset, length)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset)` - الدالة `overlayUTF8(string, replacement, offset)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - الدالة `overlayUTF8(string, replacement, offset, length)`.

<div id="operators-for-working-with-data-sets">
  ## عوامل التشغيل للتعامل مع مجموعات البيانات
</div>

راجع [عوامل التشغيل `IN`](../../sql-reference/operators/in.md) و[عامل التشغيل `EXISTS`](../../sql-reference/operators/exists.md).

<div id="in-function">
  ### الدالة in
</div>

`a IN ...` – الدالة `in(a, b)`.

<div id="notin-function">
  ### دالة notIn
</div>

`a NOT IN ...` – دالة `notIn(a, b)`.

<div id="globalin-function">
  ### الدالة globalIn
</div>

`a GLOBAL IN ...` – الدالة `globalIn(a, b)`.

<div id="globalnotin-function">
  ### دالة globalNotIn
</div>

`a GLOBAL NOT IN ...` – الدالة `globalNotIn(a, b)`.

<div id="in-subquery-function">
  ### in subquery function
</div>

`a = ANY (subquery)` – وهي الدالة `in(a, subquery)`.

<div id="notin-subquery-function">
  ### دالة notIn للاستعلامات الفرعية
</div>

`a != ANY (subquery)` – هي نفسها `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="in-subquery-function-1">
  ### الدالة in subquery function
</div>

`a = ALL (subquery)` – وهي مماثلة لـ `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="notin-subquery-function">
  ### دالة notIn للاستعلامات الفرعية
</div>

`a != ALL (subquery)` – الدالة `notIn(a, subquery)`.

**أمثلة**

استعلام باستخدام ALL:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

استعلام باستخدام ANY:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

<div id="some-all-on-arrays">
  ### `SOME` / `ALL` على المصفوفات
</div>

بالإضافة إلى صيغة الاستعلام الفرعي الموضّحة أعلاه، يمكن أن يكون الطرف الأيمن من `SOME` / `ALL` تعبيرَ مصفوفة (قيمة حرفية لمصفوفة، أو عمودًا من نوع مصفوفة، أو أي تعبير يُرجع مصفوفة). هذه هي صيغة محدِّد الكمّ للمصفوفات على طريقة PostgreSQL. ويُتعرَّف عليها أثناء التحليل وتُعاد كتابتها إلى دوال المصفوفات، لذلك لا حاجة إلى إعادة كتابتها يدويًا:

| البنية                                             | يُعاد كتابتها إلى                  |
| -------------------------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                                 | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                                 | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (any other supported operator) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (any other supported operator)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` هو محدِّد الكمّ الوجودي (وهو المرادف في SQL لـ `ANY`). وتُعامَل `=` و`<>` معاملة خاصة فتُحوَّلان إلى `has` / `NOT has` لأن لهما تنفيذًا مُحسَّنًا؛ أما الصيغة العامة فترجع إلى الدالتين عاليتَي الرتبة `arrayExists` / `arrayAll`.

يُتعرَّف على صيغة المصفوفة مع عامل المقارنة `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`، ومع محددات المقارنة بالكلمات المفتاحية `IS DISTINCT FROM` و`IS NOT DISTINCT FROM`، ومع محددات البحث في السلاسل النصية `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE`, و`REGEXP`. ولا يُتعرَّف على محددات المقارنة بالكلمات المفتاحية ومحددات البحث في السلاسل النصية إلا في صيغة المصفوفة، لا في صيغة الاستعلام الفرعي (التي تُحوَّل إلى `IN`/`NOT IN`). أما المعاملات التي لا تحمل معنى محدِّد الكمّ للمصفوفة — مثل `IN` نفسه — فلا **تُعاد** كتابتها وتحتفظ بمعناها المعتاد.

تعمل محددات البحث في السلاسل النصية لأن `MatchImpl` (التنفيذ الكامن وراء `LIKE` / `ILIKE` / `REGEXP`) يدعم سلسلةً ثابتةً يُبحث فيها مع نمط بحث غير ثابت. على سبيل المثال، تُعاد كتابة `'abc' LIKE SOME(['a%', 'b%'])` إلى `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])`، وتُعاد كتابة `'abc' NOT LIKE ALL(['x%', 'y%'])` إلى `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])`. وهذا يطابق سلسلة نصية واحدة مع عدة أنماط؛ أما إذا أردت المطابقة في تمريرة واحدة مجمّعة، فلا يزال بإمكانك استخدام دالة بحث متعددة الأنماط مثل `multiMatchAny` (التعبيرات النمطية) أو `multiSearchAny` (السلاسل الفرعية).

:::note `ANY` غير مدعوم لصيغة المصفوفة
لا يقبل الطرف الأيمن كمصفوفة إلا `SOME` و`ALL`. ويُستبعَد `ANY` لأن `any` هي أيضًا دالة تجميع، لذا فإن تعبيرًا بالشكل `expr = any(x)` يحتفظ بمعنى استدعاء الدالة. استخدم `SOME` كمحدِّد كمّ للمصفوفات.
:::

```sql title="Query"
SELECT
    3 = SOME([1, 2, 3, 4])         AS in_array,
    5 < SOME([1, 2, 6])            AS less_than_some,
    5 > ALL([1, 2, 3])             AS greater_than_all,
    'abc' LIKE SOME(['a%', 'z%'])  AS like_some;
```

```text title="Response"
┌─in_array─┬─less_than_some─┬─greater_than_all─┬─like_some─┐
│        1 │              1 │                1 │         1 │
└──────────┴────────────────┴──────────────────┴───────────┘
```

:::note يختلف التعامل مع `NULL` عن صيغة الاستعلام الفرعي
نظرًا إلى أن صيغة المصفوفة يُعادَت كتابتها في المُحلِّل النحوي (parser) — حيث لا تتوفر إعدادات الاستعلام مثل `transform_null_in`، ولا يمكن لعمود مصفوفة على مستوى الصف استخدام مسار `IN` الآمن بالنسبة إلى `NULL` الخاص بالمحلِّل — فإنها تستخدم دلالات ثنائية القيمة لكل من `has` (مع `=` / `<>`) و`arrayExists` / `arrayAll` (اللَّتين تحوِّلان نتيجة مقارنة `NULL` المجهولة إلى `0`). وقد يختلف ذلك عن صيغة الاستعلام الفرعي، إذ يُنفَّذ فيها التعامل مع `NULL` عبر `IN` / `NOT IN` ويعتمد على `transform_null_in`:

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## معاملات التعامل مع التواريخ والأوقات
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

استخرج أجزاء من تاريخ معيّن. على سبيل المثال، يمكنك استخراج الشهر من تاريخ معيّن، أو الثانية من وقت.

تحدّد المعلمة `part` الجزء المطلوب استخراجه من التاريخ. القيم التالية متاحة:

* `NANOSECOND` — النانوثانية. القيم الممكنة: 0–999999999.
* `MICROSECOND` — الميكروثانية. القيم الممكنة: 0–999999.
* `MILLISECOND` — الملّي ثانية. القيم الممكنة: 0–999.
* `SECOND` — الثانية. القيم الممكنة: 0–59.
* `MINUTE` — الدقيقة. القيم الممكنة: 0–59.
* `HOUR` — الساعة. القيم الممكنة: 0–23.
* `DAY` — يوم الشهر. القيم الممكنة: 1–31.
* `WEEK` — رقم الأسبوع وفق ISO 8601. القيم الممكنة: 1–53.
* `MONTH` — رقم الشهر. القيم الممكنة: 1–12.
* `QUARTER` — الربع. القيم الممكنة: 1–4.
* `YEAR` — السنة.
* `EPOCH` — Unix timestamp (الثواني منذ 1970-01-01 00:00:00 UTC). ملاحظة: بالنسبة إلى `DateTime64`، يُحذف الجزء الأقل من الثانية.
* `DOW` — يوم الأسبوع (متوافق مع PostgreSQL). 0 = الأحد، 6 = السبت.
* `DOY` — يوم السنة. القيم الممكنة: 1–366.
* `ISODOW` — يوم الأسبوع وفق ISO. 1 = الاثنين، 7 = الأحد.
* `ISOYEAR` — سنة ترقيم الأسابيع وفق ISO 8601.
* `CENTURY` — القرن. على سبيل المثال، تقع السنة 2024 في القرن الحادي والعشرين.
* `DECADE` — العقد (السنة مقسومة على 10). على سبيل المثال، السنة 2024 عقدها 202.
* `MILLENNIUM` — الألفية. على سبيل المثال، تقع السنة 2024 في الألفية الثالثة.
* `TIMEZONE_HOUR` — جزء الساعات الموقَّع من إزاحة UTC للمنطقة الزمنية الخاصة بالمعامل. على سبيل المثال، `+5:30` تُرجع `5`، و`-3:30` تُرجع `-3`.
* `TIMEZONE_MINUTE` — جزء الدقائق الموقَّع من إزاحة UTC للمنطقة الزمنية الخاصة بالمعامل. على سبيل المثال، `+5:30` تُرجع `30`، و`-3:30` تُرجع `-30`.

المعلمة `part` غير حساسة لحالة الأحرف.

تحدّد المعلمة `date` القيمة المطلوب معالجتها. الأنواع [Date](../../sql-reference/data-types/date.md) و[Date32](../../sql-reference/data-types/date32.md) و[DateTime](../../sql-reference/data-types/datetime.md) و[DateTime64](../../sql-reference/data-types/datetime64.md) و[Interval](../../sql-reference/data-types/special-data-types/interval.md) مدعومة. عندما تكون `date` من النوع `Interval`، يجب أن يطابق `part` المطلوب نوع interval المخزَّن (على سبيل المثال، `EXTRACT(DAY FROM INTERVAL 5 DAY)` مسموح به؛ أما `EXTRACT(HOUR FROM INTERVAL 5 DAY)` فيُرفض، لأن فواصل ClickHouse الزمنية تكون من نوع واحد فقط). وتكون نتيجة معامل `Interval` من النوع `Int64`.

أمثلة:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 5
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 30
SELECT EXTRACT(DAY   FROM INTERVAL 40 DAY);                                                -- 40
SELECT EXTRACT(MONTH FROM INTERVAL 7 MONTH);                                               -- 7
```

في المثال التالي، ننشئ جدولًا ونُدرج فيه قيمة من النوع `DateTime`.

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

يمكنك الاطلاع على المزيد من الأمثلة في [tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

<div id="interval">
  ### INTERVAL
</div>

ينشئ قيمة من النوع [Interval](../../sql-reference/data-types/special-data-types/interval.md) تُستخدم في العمليات الحسابية مع القيم من النوع [Date](../../sql-reference/data-types/date.md) و[DateTime](../../sql-reference/data-types/datetime.md).

أنواع interval:

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

يمكنك أيضًا استخدام قيمة حرفية نصية عند تعيين قيمة `INTERVAL`. على سبيل المثال، `INTERVAL 1 HOUR` مطابق لـ `INTERVAL '1 hour'` أو `INTERVAL '1' hour`.

:::tip
لا يمكن دمج intervals من أنواع مختلفة. لا يمكنك استخدام تعبيرات مثل `INTERVAL 4 DAY 1 HOUR`. حدِّد intervals بوحدات أصغر من أصغر وحدة في interval أو مساوية لها، على سبيل المثال `INTERVAL 25 HOUR`. يمكنك استخدام عمليات متتالية، كما في المثال أدناه.
:::

أمثلة:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
يُفضَّل دائمًا استخدام صياغة `INTERVAL` أو الدالة `addDays`. فعمليات الجمع أو الطرح البسيطة (بصياغة مثل `now() + ...`) لا تراعي إعدادات الوقت، مثل التوقيت الصيفي.
:::

أمثلة:

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**راجع أيضًا**

* [Interval](../../sql-reference/data-types/special-data-types/interval.md) نوع البيانات
* [toInterval](/ar/sql-reference/functions/type-conversion-functions#toIntervalYear) دوال تحويل النوع

<div id="date-time-addition">
  ### جمع التاريخ والوقت
</div>

يمكن إضافة قيمة [Date](../../sql-reference/data-types/date.md) أو [Date32](../../sql-reference/data-types/date32.md) إلى قيمة [Time](../../sql-reference/data-types/time.md) أو [Time64](../../sql-reference/data-types/time64.md) باستخدام المعامل `+`. وتكون النتيجة [DateTime](../../sql-reference/data-types/datetime.md) أو [DateTime64](../../sql-reference/data-types/datetime64.md)، وتمثل ذلك التاريخ عند الوقت المحدد من اليوم. وهذه العملية إبدالية.

يعتمد نوع النتيجة على نوعَي المعاملين:

| المعامل الأيسر | المعامل الأيمن | نوع النتيجة     |
| -------------- | -------------- | --------------- |
| `Date`         | `Time`         | `DateTime`      |
| `Date`         | `Time64(s)`    | `DateTime64(s)` |
| `Date32`       | `Time`         | `DateTime64(0)` |
| `Date32`       | `Time64(s)`    | `DateTime64(s)` |

:::note
تستخدم النتيجة [المنطقة الزمنية للجلسة](../../operations/settings/settings.md#session_timezone) (أو المنطقة الزمنية الافتراضية للخادم إذا لم يتم تعيين منطقة زمنية للجلسة). ويتحكم الإعداد [`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) في ما يحدث عندما تكون النتيجة خارج النطاق القابل للتمثيل.
:::

أمثلة:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

<div id="at-time-zone">
  ### `AT TIME ZONE` و `AT LOCAL`
</div>

تحوِّل المعاملات اللاحقة `AT TIME ZONE` و `AT LOCAL` قيمة `DateTime` أو `DateTime64` إلى منطقة زمنية مختلفة. وهما اختصار نحوي للدالة الحالية [`toTimeZone`](/ar/sql-reference/functions/date-time-functions#totimezone):

| الصياغة                  | المكافئ                        |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

يمكن أن تكون `zone` أي تعبير سلسلة ثابت يُقيَّم إلى اسم منطقة زمنية صالح (مثل: `'America/Denver'` أو `'UTC'` أو `concat('America', '/', 'Denver')`). وبما أن `AT TIME ZONE` تُختصر إلى `toTimeZone`، فإن قواعد وسيط المنطقة الزمنية نفسها تنطبق هنا: فالتعبيرات غير الثابتة، مثل مرجع العمود، تتطلب [`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments).

يستخدم `AT LOCAL` [المنطقة الزمنية للجلسة](../../operations/settings/settings.md#session_timezone) الحالية (أو المنطقة الزمنية الافتراضية للخادم إذا لم يتم تعيين منطقة زمنية للجلسة). في جداول `Distributed`، يجب تعيين `session_timezone` صراحةً؛ وعندما تكون فارغة، تكون `timeZone()` محلية لكل shard ولا يمكن استخدامها كوسيط ثابت لـ `toTimeZone`، مما يؤدي إلى استثناء `ILLEGAL_COLUMN`.

:::note
على خلاف PostgreSQL، حيث يعيد `timestamp without time zone AT TIME ZONE zone` تفسير قيمة وقت الحائط على أنها ضمن المنطقة الزمنية المحددة قبل التحويل، يحتفظ ClickHouse دائمًا بنفس النقطة الزمنية المطلقة ويغيّر فقط تسمية المنطقة الزمنية المستخدمة للعرض. وكلتا الصيغتين مكافئتان لـ `toTimeZone` ولا تغيّران قيمة `timestamp` الأساسية.
:::

تبلغ أسبقية المعامل `AT TIME ZONE` القيمة 13 (أعلى من `*`/`/`/`%` عند 12، وأعلى من `+`/`-` عند 11)، بما يتوافق مع PostgreSQL. وهذا يعني أن `a * ts AT TIME ZONE 'tz'` يُفسَّر على أنه `a * (ts AT TIME ZONE 'tz')`، وأن `ts + interval AT TIME ZONE 'tz'` يُفسَّر على أنه `ts + (interval AT TIME ZONE 'tz')`. لتطبيق تحويل المنطقة الزمنية بعد العمليات الحسابية، استخدم أقواسًا صريحة:

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

أمثلة:

```sql
SET session_timezone = 'UTC';

SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), 'America/Denver')─┐
│ 2001-02-16 13:38:40                                              │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), timeZone())─┐
│ 2001-02-16 20:38:40                                        │
└────────────────────────────────────────────────────────────┘
```

**راجع أيضًا**

* [`toTimeZone`](/ar/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/ar/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## المعامل المنطقي AND
</div>

الصياغة `SELECT a AND b` — يحسب الاقتران المنطقي بين `a` و`b` باستخدام الدالة [and](/ar/sql-reference/functions/logical-functions#and).

<div id="logical-or-operator">
  ## معامل التشغيل المنطقي OR
</div>

الصياغة `SELECT a OR b` — تحسب الفصل المنطقي بين `a` و`b` باستخدام الدالة [or](/ar/sql-reference/functions/logical-functions#or).

<div id="logical-negation-operator">
  ## معامل النفي المنطقي
</div>

الصيغة `SELECT NOT a` — تحسب النفي المنطقي لـ `a` باستخدام الدالة [not](/ar/sql-reference/functions/logical-functions#not).

<div id="conditional-operator">
  ## المعامل الشرطي
</div>

`a ? b : c` – الدالة `if(a, b, c)`.

ملاحظة:

يحسب المعامل الشرطي قيمتَي b و c، ثم يتحقق مما إذا كان الشرط a متحققًا، ثم يعيد القيمة المقابلة. إذا كانت `b` أو `C` دالة [arrayJoin()](/ar/sql-reference/functions/array-join)، فسيُكرَّر كل صف بغض النظر عن الشرط &quot;a&quot;.

<div id="conditional-expression">
  ## التعبير الشرطي
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

إذا كان `x` محددًا، فستُستخدم الدالة `transform(x, [a, ...], [b, ...], c)`. وإلا فستُستخدم `multiIf(a, b, ..., c)`.

إذا لم تتضمن الصيغة عبارة `ELSE c`، فستكون القيمة الافتراضية `NULL`.

لا تعمل الدالة `transform` مع `NULL`.

<div id="concatenation-operator">
  ## معامل دمج السلاسل
</div>

`s1 || s2` – الدالة `concat(s1, s2)`.

<div id="lambda-creation-operator">
  ## معامل إنشاء لامبدا
</div>

`x -> expr` – الدالة `lambda(x, expr)`.

لا تملك المعاملات التالية أسبقية لأنها أقواس:

<div id="array-creation-operator">
  ## معامل إنشاء المصفوفة
</div>

`[x1, ...]` – الدالة `array(x1, ...).`

<div id="tuple-creation-operator">
  ## معامل إنشاء Tuple
</div>

`(x1, x2, ...)` – الدالة `tuple(x2, x2, ...)`.

<div id="associativity">
  ## الترابطية
</div>

جميع المعاملات الثنائية مترابطة من اليسار. على سبيل المثال، يُحوَّل `1 + 2 + 3` إلى `plus(plus(1, 2), 3)`.
وأحيانًا قد لا يعمل ذلك كما تتوقع. على سبيل المثال، ستؤدي `SELECT 4 > 2 > 3` إلى 0.

ولتحسين الكفاءة، تقبل الدالتان `and` و`or` أي عدد من الوسيطات. وتُحوَّل السلاسل المقابلة من المعاملين `AND` و`OR` إلى استدعاء واحد لهاتين الدالتين.

<div id="checking-for-null">
  ## التحقق من `NULL`
</div>

يدعم ClickHouse المعاملين `IS NULL` و`IS NOT NULL`.

<div id="is_null">
  ### IS NULL
</div>

* بالنسبة إلى قيم النوع [Nullable](../../sql-reference/data-types/nullable.md)، يُرجع العامل `IS NULL` ما يلي:
  * `1` إذا كانت القيمة `NULL`.
  * `0` في غير ذلك.
* بالنسبة إلى القيم الأخرى، يُرجع العامل `IS NULL` دائمًا `0`.

يمكن تحسين ذلك بتمكين الإعداد [optimize&#95;functions&#95;to&#95;subcolumns](/ar/operations/settings/settings#optimize_functions_to_subcolumns). عند ضبط `optimize_functions_to_subcolumns = 1`، لا تقرأ الدالة سوى العمود الفرعي [null](../../sql-reference/data-types/nullable.md#finding-null) بدلًا من قراءة بيانات العمود بالكامل ومعالجتها. ويتحوّل الاستعلام `SELECT n IS NULL FROM table` إلى `SELECT n.null FROM TABLE`.

{/* */ }

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

<div id="is_not_null">
  ### IS NOT NULL
</div>

* بالنسبة إلى القيم من النوع [Nullable](../../sql-reference/data-types/nullable.md)، يُرجع العامل `IS NOT NULL` ما يلي:
  * `0` إذا كانت القيمة `NULL`.
  * `1` في غير ذلك.
* بالنسبة إلى القيم الأخرى، يُرجع العامل `IS NOT NULL` دائمًا `1`.

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

يمكن تحسين الأداء بتمكين الإعداد [optimize&#95;functions&#95;to&#95;subcolumns](/ar/operations/settings/settings#optimize_functions_to_subcolumns). عند تعيين `optimize_functions_to_subcolumns = 1`، لا تقرأ الدالة سوى العمود الفرعي [null](../../sql-reference/data-types/nullable.md#finding-null) بدلًا من قراءة بيانات العمود بالكامل ومعالجتها. ويتحول الاستعلام `SELECT n IS NOT NULL FROM table` إلى `SELECT NOT n.null FROM TABLE`.

<div id="checking-boolean-values">
  ## التحقق من القيم المنطقية
</div>

يدعم ClickHouse معاملات `IS TRUE` و`IS FALSE` و`IS UNKNOWN` و`IS NOT TRUE` و`IS NOT FALSE` و`IS NOT UNKNOWN`.
وتُستخدم مع تعبيرات [Bool](../../sql-reference/data-types/boolean.md) و`Nullable(Bool)`.

* يُرجع `expr IS TRUE` القيمة `1` فقط إذا كانت قيمة `expr` هي `true`.
* يُرجع `expr IS FALSE` القيمة `1` فقط إذا كانت قيمة `expr` هي `false`.
* يُرجع `expr IS UNKNOWN` القيمة `1` فقط إذا كانت قيمة `expr` هي `NULL`.
* يُرجع `expr IS NOT TRUE` القيمة `1` إذا كانت قيمة `expr` هي `false` أو `NULL`.
* يُرجع `expr IS NOT FALSE` القيمة `1` إذا كانت قيمة `expr` هي `true` أو `NULL`.
* يُرجع `expr IS NOT UNKNOWN` القيمة `1` إذا لم تكن قيمة `expr` هي `NULL`.

بالنسبة إلى التعبيرات المنطقية، فإن `IS UNKNOWN` تكافئ `IS NULL`، و`IS NOT UNKNOWN` تكافئ `IS NOT NULL`.

{/* */ }

```sql
CREATE TABLE t_bool (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (true), (false), (NULL);

SELECT
    x,
    x IS TRUE,
    x IS FALSE,
    x IS UNKNOWN,
    x IS NOT TRUE,
    x IS NOT FALSE,
    x IS NOT UNKNOWN
FROM t_bool;
```