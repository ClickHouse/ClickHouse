---
description: 'توثيق دالة arrayJoin'
sidebar_label: 'arrayJoin'
slug: /sql-reference/functions/array-join
title: 'دالة arrayJoin'
doc_type: 'reference'
---

هذه دالة غير مألوفة للغاية.

الدوال العادية لا تغيّر مجموعة الصفوف، بل تغيّر فقط القيم داخل كل صف (`map`).
أما الدوال التجميعية فتضغط مجموعة من الصفوف (fold أو reduce).
وتأخذ الدالة `arrayJoin` كل صف وتحوّله إلى مجموعة من الصفوف (unfold).

تأخذ هذه الدالة مصفوفة كوسيط، وتُمرِّر الصف الأصلي إلى عدة صفوف بعدد العناصر الموجودة في المصفوفة.
وتُنسخ جميع القيم في الأعمدة كما هي، باستثناء القيم في العمود الذي تُطبَّق عليه هذه الدالة، إذ تُستبدل بقيمة العنصر المقابل من المصفوفة.

:::note
إذا كانت المصفوفة فارغة، فلن تُنتج `arrayJoin` أي صفوف.
ولإرجاع صف واحد يحتوي على القيمة الافتراضية لنوع المصفوفة، يمكنك إحاطتها بـ [emptyArrayToSingle](./array-functions.md#emptyArrayToSingle)، على سبيل المثال: `arrayJoin(emptyArrayToSingle(...))`.
:::

على سبيل المثال:

```sql title="Query"
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

```text title="Response"
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

تؤثر الدالة `arrayJoin` في جميع أجزاء الاستعلام، بما في ذلك قسم `WHERE`. لاحظ أن نتيجة الاستعلام أدناه هي `2`، مع أن الاستعلام الفرعي أعاد صفًا واحدًا فقط.

```sql title="Query"
SELECT sum(1) AS impressions
FROM
(
    SELECT ['Istanbul', 'Berlin', 'Babruysk'] AS cities
)
WHERE arrayJoin(cities) IN ['Istanbul', 'Berlin'];
```

```text title="Response"
┌─impressions─┐
│           2 │
└─────────────┘
```

يمكن أن يستخدم الاستعلام عدة دوال `arrayJoin`. في هذه الحالة، تُجرى عملية التحويل عدة مرات، وتتضاعف الصفوف.
على سبيل المثال:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    arrayJoin(cities) AS city,
    arrayJoin(browsers) AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           2 │ Istanbul │ Chrome  │
│           1 │ Istanbul │ Firefox │
│           2 │ Berlin   │ Chrome  │
│           1 │ Berlin   │ Firefox │
│           2 │ Babruysk │ Chrome  │
│           1 │ Babruysk │ Firefox │
└─────────────┴──────────┴─────────┘
```

<div id="important-note">
  ### أفضل الممارسات
</div>

قد لا يؤدي استخدام `arrayJoin` عدة مرات مع التعبير نفسه إلى النتائج المتوقعة بسبب إزالة التعبيرات الفرعية المشتركة.
في هذه الحالات، فكّر في تعديل تعبيرات المصفوفة المتكررة بإضافة عمليات لا تؤثر في نتيجة الـ join. على سبيل المثال: `arrayJoin(arraySort(arr))`, `arrayJoin(arrayConcat(arr, []))`

مثال:

```sql title="Query"
SELECT
    arrayJoin(dice) AS first_throw,
    /* arrayJoin(dice) as second_throw */ -- is technically correct, but will annihilate result set
    arrayJoin(arrayConcat(dice, [])) AS second_throw -- intentionally changed expression to force re-evaluation
FROM (
    SELECT [1, 2, 3, 4, 5, 6] AS dice
);
```

لاحظ صيغة [`ARRAY JOIN`](../statements/select/array-join.md) في استعلام SELECT، إذ تتيح إمكانات أوسع.
يتيح `ARRAY JOIN` تحويل عدة مصفوفات لها العدد نفسه من العناصر دفعة واحدة.

مثال:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    city,
    browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
ARRAY JOIN
    cities AS city,
    browsers AS browser
GROUP BY
    2,
    3
```

```text title="Response"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

أو يمكنك استخدام [`Tuple`](../data-types/tuple.md)

مثال:

```sql title="Query"
SELECT
    sum(1) AS impressions,
    (arrayJoin(arrayZip(cities, browsers)) AS t).1 AS city,
    t.2 AS browser
FROM
(
    SELECT
        ['Istanbul', 'Berlin', 'Babruysk'] AS cities,
        ['Firefox', 'Chrome', 'Chrome'] AS browsers
)
GROUP BY
    2,
    3
```

```text title="Row"
┌─impressions─┬─city─────┬─browser─┐
│           1 │ Istanbul │ Firefox │
│           1 │ Berlin   │ Chrome  │
│           1 │ Babruysk │ Chrome  │
└─────────────┴──────────┴─────────┘
```

اسم `arrayJoin` في ClickHouse مستمد من تشابهه المفاهيمي مع عملية JOIN، ولكن عند تطبيقها على المصفوفات داخل صف واحد. فبينما تجمع عمليات JOIN التقليدية صفوفًا من جداول مختلفة، فإن `arrayJoin` &quot;تضم&quot; كل عنصر من عناصر المصفوفة داخل صف واحد، فتنتج عدة صفوف — صفًا لكل عنصر في المصفوفة — مع تكرار قيم الأعمدة الأخرى. كما يوفّر ClickHouse أيضًا صياغة العبارة [`ARRAY JOIN`](/ar/sql-reference/statements/select/array-join)، مما يجعل هذه الصلة بعمليات JOIN التقليدية أوضح، من خلال استخدام مصطلحات SQL JOIN المألوفة. ويُشار إلى هذه العملية أيضًا باسم &quot;بسط&quot; المصفوفة، لكن يُستخدم مصطلح &quot;join&quot; في كلٍّ من اسم الدالة والعبارة لأنه يشبه ضم الجدول إلى عناصر المصفوفة، بما يؤدي فعليًا إلى توسيع مجموعة البيانات بطريقة مشابهة لعملية JOIN.