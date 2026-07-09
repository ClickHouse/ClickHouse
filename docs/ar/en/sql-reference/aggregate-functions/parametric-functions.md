---
description: 'توثيق الدوال التجميعية ذات المعلمات'
sidebar_label: 'ذات المعلمات'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: 'الدوال التجميعية ذات المعلمات'
doc_type: 'مرجع'
---

يمكن لبعض الدوال التجميعية أن تقبل ليس فقط أعمدة الوسائط (المستخدمة للضغط)، بل أيضًا مجموعة من المعلمات، وهي ثوابت تُستخدم للتهيئة. وتتكوّن الصياغة من زوجين من الأقواس بدلًا من زوج واحد: الأول للمعلمات، والثاني للوسائط.

<div id="histogram">
  ## مُدرَّج تكراري
</div>

يحسب مُدرَّجًا تكراريًا تكيّفيًا. ولا يضمن نتائج دقيقة.

```sql
histogram(number_of_bins)(values)
```

تستخدم الدالة [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). وتُعدَّل حدود فئات المُدرَّج التكراري مع دخول بيانات جديدة إلى الدالة. وفي الحالة المعتادة، لا تكون عروض الفئات متساوية.

**الوسائط**

`values` — [تعبير](/ar/sql-reference/syntax#expressions) ينتج عنه قيم الإدخال.

**المعلمات**

`number_of_bins` — الحد الأقصى لعدد الفئات في المُدرَّج التكراري. تحسب الدالة عدد الفئات تلقائيًا. وتحاول الوصول إلى العدد المحدد من الفئات، ولكن إذا تعذّر ذلك، تستخدم عددًا أقل من الفئات.

**القيم المُعادة**

* [Array](../../sql-reference/data-types/array.md) من [Tuples](../../sql-reference/data-types/tuple.md) بالتنسيق التالي:

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — الحد الأدنى للفئة.
  * `upper` — الحد الأعلى للفئة.
  * `height` — الارتفاع المحسوب للفئة.

**مثال**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

يمكنك عرض مُدرَّج تكراري باستخدام الدالة [bar](/ar/sql-reference/functions/other-functions#bar)، على سبيل المثال:

```sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

في هذه الحالة، ينبغي أن تتذكر أنك لا تعرف حدود فئات المُدرَّج التكراري.

<div id="sequencematch">
  ## sequenceMatch
</div>

يتحقق مما إذا كان التسلسل يتضمن سلسلة أحداث تطابق النمط.

**البنية**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
قد تَرِد الأحداث التي تقع في الثانية نفسها ضمن التسلسل بترتيب غير محدد، مما يؤثر في النتيجة.
:::

**الوسيطات**

* `timestamp` — العمود الذي يُعتبر محتويًا على بيانات الوقت. أنواع البيانات المعتادة هي `Date` و`DateTime`. يمكنك أيضًا استخدام أيٍّ من أنواع بيانات [UInt](../../sql-reference/data-types/int-uint.md) المدعومة.

* `cond1`, `cond2` — شروط تصف سلسلة الأحداث. نوع البيانات: `UInt8`. يمكنك تمرير ما يصل إلى 32 وسيطة شرطية. لا تأخذ الدالة في الاعتبار إلا الأحداث الموصوفة في هذه الشروط. إذا كان التسلسل يحتوي على بيانات غير موصوفة في أي شرط، فإن الدالة تتخطاها.

**المعلمات**

* `pattern` — سلسلة النمط. راجع [بنية النمط](#pattern-syntax).

**القيم المُعادة**

* 1، إذا تمت مطابقة النمط.
* 0، إذا لم تتم مطابقة النمط.

النوع: `UInt8`.

<div id="pattern-syntax">
  #### بنية النمط
</div>

* `(?N)` — يطابق وسيطة الشرط في الموضع `N`. تُرقَّم الشروط ضمن النطاق `[1, 32]`. على سبيل المثال، يطابق `(?1)` الوسيطة المُمرَّرة إلى المعلَمة `cond1`.

* `.*` — يطابق أي عدد من الأحداث. لا تحتاج إلى وسائط شرطية لمطابقة هذا العنصر من النمط.

* `(?t operator value)` — يحدِّد الفاصل الزمني بالثواني بين حدثين. على سبيل المثال، يطابق النمط `(?1)(?t>1800)(?2)` الأحداث التي يفصل بينها أكثر من 1800 ثانية. ويمكن أن يرد بين هذين الحدثين أي عدد من الأحداث. يمكنك استخدام العوامل `>=`, `>`, `<`, `<=`, `==`.

**أمثلة**

لنفترض وجود بيانات في الجدول `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

نفّذ الاستعلام:

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

عثرت الدالة على سلسلة الأحداث التي يأتي فيها الرقم 2 بعد الرقم 1. وقد تخطّت الرقم 3 الواقع بينهما، لأن هذا الرقم غير موصوف على أنه حدث. وإذا أردنا أخذ هذا الرقم في الحسبان عند البحث عن سلسلة الأحداث الواردة في المثال، فينبغي أن نضع له شرطًا.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

في هذه الحالة، لم تتمكن الدالة من العثور على سلسلة الأحداث المطابقة للنمط، لأن الحدث رقم 3 وقع بين 1 و2. ولو تحققنا في الحالة نفسها من الشرط الخاص بالرقم 4، لطابق التسلسل النمط.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**راجع أيضًا**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

يحسب عدد سلاسل الأحداث التي تطابق النمط. تبحث الدالة في سلاسل أحداث غير متداخلة، وتبدأ البحث عن السلسلة التالية بعد مطابقة السلسلة الحالية.

:::note
قد تَرِد الأحداث التي تقع في الثانية نفسها ضمن التسلسل بترتيب غير محدد، مما قد يؤثر في النتيجة.
:::

**الصيغة**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**الوسيطات**

* `timestamp` — العمود الذي يُعدّ محتويًا على بيانات الوقت. أنواع البيانات المعتادة هي `Date` و`DateTime`. ويمكنك أيضًا استخدام أيًا من أنواع بيانات [UInt](../../sql-reference/data-types/int-uint.md) المدعومة.

* `cond1`, `cond2` — شروط تصف سلسلة الأحداث. نوع البيانات: `UInt8`. يمكنك تمرير ما يصل إلى 32 وسيطًا للشروط. لا تأخذ الدالة في الاعتبار إلا الأحداث الموصوفة في هذه الشروط. وإذا احتوى التسلسل على بيانات غير موصوفة في أي شرط، فإن الدالة تتخطاها.

**المعلمات**

* `pattern` — سلسلة النمط. راجع [بنية النمط](#pattern-syntax).

**القيم المعادة**

* عدد سلاسل الأحداث غير المتداخلة التي تمت مطابقتها.

النوع: `UInt64`.

**مثال**

لننظر إلى البيانات في الجدول `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

احسب عدد المرات التي يرد فيها الرقم 2 بعد الرقم 1، مع وجود أي عدد من الأرقام الأخرى بينهما:

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

تُرجِع الطوابع الزمنية للأحداث في أطول سلاسل الأحداث التي طابقت النمط.

:::note
قد ترد الأحداث التي تقع في الثانية نفسها ضمن التسلسل بترتيب غير معرّف، مما يؤثر في النتيجة.
:::

**البنية**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**الوسيطات**

* `timestamp` — العمود الذي يُعتبر محتويًا على بيانات زمنية. أنواع البيانات الشائعة هي `Date` و `DateTime`. ويمكنك أيضًا استخدام أيٍّ من أنواع بيانات [UInt](../../sql-reference/data-types/int-uint.md) المدعومة.

* `cond1`, `cond2` — الشروط التي تصف سلسلة الأحداث. نوع البيانات: `UInt8`. يمكنك تمرير ما يصل إلى 32 وسيطة شرطية. لا تأخذ الدالة في الاعتبار إلا الأحداث الموصوفة في هذه الشروط. وإذا كان التسلسل يحتوي على بيانات غير موصوفة في أي شرط، فإن الدالة تتجاوزها.

**المعلمات**

* `pattern` — سلسلة النمط. راجع [بنية النمط](#pattern-syntax).

**القيم المعادة**

* مصفوفة من الطوابع الزمنية لوسيطات الشروط المتطابقة (?N) من سلسلة الأحداث. يتطابق الموضع في المصفوفة مع موضع وسيطة الشرط في النمط

النوع: Array.

**مثال**

لنفترض وجود بيانات في الجدول `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

إرجاع الطوابع الزمنية لأحداث أطول سلسلة

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**راجع أيضًا**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

يبحث عن سلاسل الأحداث ضمن نافذة زمنية منزلقة، ويحسب الحد الأقصى لعدد الأحداث التي وقعت في السلسلة.

تعمل الدالة وفقًا للخوارزمية التالية:

* تبحث الدالة عن البيانات التي تُفعِّل الشرط الأول في السلسلة، وتضبط عدّاد الأحداث على 1. وهذه هي اللحظة التي تبدأ فيها النافذة المنزلقة.

* إذا وقعت أحداث من السلسلة بشكل متتابع داخل النافذة، يُزاد العدّاد. وإذا انقطع تسلسل الأحداث، فلا يُزاد العدّاد.

* إذا كانت البيانات تحتوي على عدة سلاسل أحداث بمراحل إكمال متفاوتة، فلن تُخرج الدالة إلا طول أطول سلسلة.

**الصيغة**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**الوسائط**

* `timestamp` — اسم العمود الذي يحتوي على الطابع الزمني. أنواع البيانات المدعومة: [Date](../../sql-reference/data-types/date.md)، و[DateTime](/ar/sql-reference/data-types/datetime)، وغيرها من أنواع الأعداد الصحيحة غير الموقَّعة (لاحظ أنه رغم أن `timestamp` يدعم النوع `UInt64`، فإن قيمته لا يمكن أن تتجاوز الحد الأقصى لـ Int64، وهو 2^63 - 1).
* `cond` — الشروط أو البيانات التي تصف سلسلة الأحداث. [UInt8](../../sql-reference/data-types/int-uint.md).

**المعلمات**

* `window` — طول النافذة المنزلقة، وهي الفاصل الزمني بين الشرط الأول والشرط الأخير. تعتمد وحدة `window` على `timestamp` نفسه، ولذلك تختلف. ويُحدَّد ذلك باستخدام التعبير `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.
* `mode` — وسيط اختياري. يمكن تعيين وضع واحد أو أكثر.
  * `'strict_deduplication'` — إذا تحقق الشرط نفسه ضمن تسلسل الأحداث، فإن تكرار هذا الحدث يوقف المعالجة اللاحقة. ملاحظة: قد يعمل بشكل غير متوقع إذا تحققت عدة شروط للحدث نفسه.
  * `'strict_order'` — لا تسمح بتداخل أحداث أخرى. على سبيل المثال، في الحالة `A->B->D->C`، يتوقف عن العثور على `A->B->C` عند `D`، ويكون الحد الأقصى لمستوى الحدث 2.
  * `'strict_increase'` — طبّق الشروط فقط على الأحداث ذات الطوابع الزمنية المتزايدة تصاعديًا بشكل صارم.
  * `'strict_once'` — احسب كل حدث مرة واحدة فقط في السلسلة حتى إذا استوفى الشرط عدة مرات.
  * `'allow_reentry'` — تجاهل الأحداث التي تخالف الترتيب الصارم. على سبيل المثال، في الحالة A-&gt;A-&gt;B-&gt;C، يعثر على A-&gt;B-&gt;C بتجاهل A الزائدة، ويكون الحد الأقصى لمستوى الحدث 3.

**القيمة المعادة**

الحد الأقصى لعدد الشروط المتتالية التي تم تفعيلها من السلسلة ضمن النافذة الزمنية المنزلقة.
تُحلَّل جميع السلاسل في التحديد.

النوع: `Integer`.

**مثال**

حدِّد ما إذا كانت فترة زمنية معينة كافية لكي يختار المستخدم هاتفًا ويشتريه مرتين في المتجر الإلكتروني.

عيّن سلسلة الأحداث التالية:

1. سجّل المستخدم الدخول إلى حسابه في المتجر (`eventID = 1003`).
2. يبحث المستخدم عن هاتف (`eventID = 1007, product = 'phone'`).
3. قدّم المستخدم طلبًا (`eventID = 1009`).
4. أعاد المستخدم تقديم الطلب (`eventID = 1010`).

جدول الإدخال:

```text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

اكتشف إلى أي مرحلة استطاع المستخدم `user_id` الوصول ضمن السلسلة خلال فترة ما بين يناير وفبراير من عام 2019.

```sql title="Query"
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**مثال على وضع allow&#95;reentry**

يوضح هذا المثال كيفية عمل وضع `allow_reentry` مع أنماط عودة المستخدم:

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

تأخذ الدالة كوسائط مجموعة من الشروط، من 1 إلى 32 وسيطًا من النوع `UInt8`، تشير إلى ما إذا كان شرط معيّن قد تحقق للحدث.
يمكن تحديد أي شرط كوسيط (كما في [WHERE](/ar/sql-reference/statements/select/where)).

تُطبَّق الشروط، باستثناء الشرط الأول، على شكل أزواج: فتكون نتيجة الشرط الثاني `true` إذا كان الشرطان الأول والثاني `true`، وتكون نتيجة الشرط الثالث `true` إذا كان الشرطان الأول والثالث `true`، وهكذا.

**الصيغة**

```sql
retention(cond1, cond2, ..., cond32);
```

**الوسائط**

* `cond` — تعبير يُرجع نتيجة من نوع `UInt8` ‏(1 أو 0).

**القيمة المُعادة**

مصفوفة من القيم 1 أو 0.

* 1 — تم استيفاء الشرط لهذا الحدث.
* 0 — لم يتم استيفاء الشرط لهذا الحدث.

النوع: `UInt8`.

**مثال**

لنأخذ مثالًا على حساب الدالة `retention` لتحديد عدد زيارات الموقع.

**1.** أنشئ جدولًا لتوضيح هذا المثال.

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

جدول الإدخال:

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** جمّع المستخدمين حسب المعرّف الفريد `uid` باستخدام الدالة `retention`.

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** احسب إجمالي عدد زيارات الموقع يوميًا.

```sql title="Query"
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

حيث:

* `r1`- عدد الزوار الفريدين الذين زاروا الموقع خلال 2020-01-01 (الشرط `cond1`).
* `r2`- عدد الزوار الفريدين الذين زاروا الموقع خلال فترة زمنية محددة بين 2020-01-01 و2020-01-02 (الشرطان `cond1` و`cond2`).
* `r3`- عدد الزوار الفريدين الذين زاروا الموقع خلال فترة زمنية محددة في يومي 2020-01-01 و2020-01-03 (الشرطان `cond1` و`cond3`).

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

تحسب عدد القيم المختلفة للمعامل حتى حدّ معيّن، `N`. إذا كان عدد قيم المعامل المختلفة أكبر من `N`، فستُرجع هذه الدالة `N` + 1، وإلا فستحسب القيمة الدقيقة.

يُنصح باستخدامها مع القيم الصغيرة لـ `N`، حتى 10. الحد الأقصى لقيمة `N` هو 100.

بالنسبة إلى حالة دالة التجميع، تستخدم هذه الدالة مقدارًا من الذاكرة يساوي 1 + `N` * حجم قيمة واحدة بالبايت.
وعند التعامل مع السلاسل النصية، تخزّن هذه الدالة قيمة hash غير مخصّصة للتشفير بحجم 8 بايت؛ ويكون الحساب تقريبيًا للسلاسل النصية.

على سبيل المثال، إذا كان لديك جدول يسجّل كل استعلام بحث يجريه المستخدمون على موقعك الإلكتروني. يمثّل كل صف في الجدول استعلام بحث واحدًا، مع أعمدة لمعرّف المستخدم، واستعلام البحث، والطابع الزمني للاستعلام. يمكنك استخدام `uniqUpTo` لإنشاء تقرير يعرض فقط الكلمات المفتاحية التي نتج عنها 5 مستخدمين فريدين على الأقل.

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

تحسب `uniqUpTo(4)(UserID)` عدد قيم `UserID` الفريدة لكل `SearchPhrase`، لكنها لا تحسب سوى 4 قيم فريدة كحد أقصى. إذا كان هناك أكثر من 4 قيم `UserID` فريدة لـ `SearchPhrase`، فستُرجع الدالة 5 ‏(4 + 1). بعد ذلك، تُصفّي عبارة `HAVING` قيم `SearchPhrase` التي يكون فيها عدد قيم `UserID` الفريدة أقل من 5. وسيعطيك هذا قائمة بالكلمات المفتاحية للبحث التي استخدمها ما لا يقل عن 5 مستخدمين فريدين.

<div id="summapfiltered">
  ## sumMapFiltered
</div>

تعمل هذه الدالة بالطريقة نفسها التي تعمل بها [sumMap](/ar/sql-reference/aggregate-functions/reference/summap)، إلا أنها تقبل أيضًا مصفوفة من المفاتيح لاستخدامها في التصفية كمعامل. ويكون ذلك مفيدًا بشكل خاص عند التعامل مع كاردينالية عالية من القيم الفريدة للمفاتيح.

**البنية**

`sumMapFiltered(keys_to_keep)(keys, values)`

**المعاملات**

* `keys_to_keep`: ‏[Array](../data-types/array.md) من المفاتيح لاستخدامها في التصفية.
* `keys`: ‏[Array](../data-types/array.md) من المفاتيح.
* `values`: ‏[Array](../data-types/array.md) من القيم.

**القيمة المعادة**

* تُرجع tuple من مصفوفتين: المفاتيح بترتيب مفروز، والقيم المجمعة للمفاتيح المقابلة.

**مثال**

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

تعمل هذه الدالة بالطريقة نفسها التي تعمل بها [sumMap](/ar/sql-reference/aggregate-functions/reference/summap)، باستثناء أنها تقبل أيضًا مصفوفة من المفاتيح للتصفية بها كمعلمة. ويمكن أن يكون هذا مفيدًا بشكل خاص عند العمل مع عدد كبير من المفاتيح ذات كاردينالية عالية. وهي تختلف عن الدالة [sumMapFiltered](#summapfiltered) في أنها تُجري الجمع مع overflow — أي إنها تُرجع نفس نوع البيانات لعملية الجمع مثل نوع بيانات الوسيط.

**الصياغة**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**المعلمات**

* `keys_to_keep`: ‏[Array](../data-types/array.md) من المفاتيح للتصفية بها.
* `keys`: ‏[Array](../data-types/array.md) من المفاتيح.
* `values`: ‏[Array](../data-types/array.md) من القيم.

**القيمة المعادة**

* تُرجع tuple من مصفوفتين: المفاتيح بترتيب فرز، والقيم المجمّعة للمفاتيح المقابلة.

**مثال**

في هذا المثال، ننشئ جدولًا باسم `sum_map`، ثم نُدرج فيه بعض البيانات، وبعد ذلك نستخدم كلًا من `sumMapFilteredWithOverflow` و`sumMapFiltered` والدالة `toTypeName` لمقارنة النتيجة. وبما أن `requests` كان من النوع `UInt8` في الجدول المُنشأ، فقد قام `sumMapFiltered` بترقية نوع القيم المجمّعة إلى `UInt64` لتجنب overflow، بينما أبقى `sumMapFilteredWithOverflow` النوع `UInt8`، وهو غير كافٍ لتخزين النتيجة — أي إن overflow قد حدث.

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

تعيد قيمة الحدث التالي الذي طابق سلسلة الأحداث.

*دالة تجريبية، فعِّلها باستخدام `SET allow_experimental_funnel_functions = 1`.*

**البنية**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**المعلمات**

* `direction` — يُستخدم للتنقل بين الاتجاهات.
  * forward — الانتقال إلى الأمام.
  * backward — الانتقال إلى الخلف.

* `base` — يُستخدم لتعيين النقطة الأساسية.
  * head — تعيين النقطة الأساسية إلى الحدث الأول.
  * tail — تعيين النقطة الأساسية إلى الحدث الأخير.
  * first&#95;match — تعيين النقطة الأساسية إلى أول `event1` مطابق.
  * last&#95;match — تعيين النقطة الأساسية إلى آخر `event1` مطابق.

**الوسيطات**

* `timestamp` — اسم العمود الذي يحتوي على الطابع الزمني. أنواع البيانات المدعومة: [Date](../../sql-reference/data-types/date.md)، و[DateTime](/ar/sql-reference/data-types/datetime)، وأنواع الأعداد الصحيحة غير الموقعة الأخرى.
* `event_column` — اسم العمود الذي يحتوي على قيمة الحدث التالي المراد إرجاعها. أنواع البيانات المدعومة: [String](../../sql-reference/data-types/string.md) و[Nullable(String)](../../sql-reference/data-types/nullable.md).
* `base_condition` — الشرط الذي يجب أن تستوفيه النقطة الأساسية.
* `event1`, `event2`, ... — شروط تصف سلسلة الأحداث. [UInt8](../../sql-reference/data-types/int-uint.md).

**القيم المعادة**

* `event_column[next_index]` — إذا تمت مطابقة النمط وكانت القيمة التالية موجودة.
* `NULL` - إذا لم تتم مطابقة النمط أو لم تكن القيمة التالية موجودة.

النوع: [Nullable(String)](../../sql-reference/data-types/nullable.md).

**مثال**

يمكن استخدامه عندما تكون الأحداث A-&gt;B-&gt;C-&gt;D-&gt;E وتريد معرفة الحدث الذي يلي B-&gt;C، وهو D.

عبارة الاستعلام التي تبحث عن الحدث الذي يلي A-&gt;B:

```sql title="Query"
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**سلوك `forward` و`head`**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // Base point, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // Base point, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // Base point, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**سلوك `backward` و `tail`**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // Base point, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // Base point, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point, Matched with Gift
1970-01-01 09:00:04    3   Basket // Base point, Matched with Basket
```

**سلوك `forward` و `first_match`**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // Base point
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**سلوك `backward` و`last_match`**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // Base point
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // Base point
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // Base point
1970-01-01 09:00:04    3   Basket
```

**سلوك `base_condition`**

```sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be base point because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be base point because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be base point because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // Base point
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // Base point
 1970-01-01 09:00:04    1   B      ref1 // This row can not be base point because the ref column unmatched with 'ref2'.
```