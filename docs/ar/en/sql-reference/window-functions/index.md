---
description: 'صفحة نظرة عامة على دوال النوافذ'
sidebar_label: 'دوال النوافذ'
sidebar_position: 1
slug: /sql-reference/window-functions/
title: 'دوال النوافذ'
doc_type: 'reference'
---

تتيح لك دوال النوافذ إجراء عمليات حسابية عبر مجموعة من الصفوف المرتبطة بالصف الحالي.
ويمكن استخدامها لإجراء عمليات حسابية مشابهة لتلك التي تُجرى باستخدام الدوال التجميعية، لكنها تختلف في أنها لا تؤدي إلى تجميع الصفوف في ناتج واحد، بل تظل الصفوف الفردية مُعادة.

<div id="standard-window-functions">
  ## دوال النوافذ القياسية
</div>

يدعم ClickHouse صياغة SQL القياسية للنوافذ ودوال النوافذ.
يوضح الجدول أدناه الميزات المدعومة حاليًا:

| الميزة                                                                  | مدعومة؟ | تعليق                                                                                                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| تعريف نافذة مخصص (`count(*) OVER (PARTITION BY id ORDER BY time DESC)`) | ✅       |                                                                                                                                                                                                                                                                                                                                                                                      |
| تعبيرات تتضمن دوال النوافذ، مثل `(count(*) OVER ()) / 2`                | ✅       |                                                                                                                                                                                                                                                                                                                                                                                      |
| clause `WINDOW` (`SELECT ... FROM table WINDOW w AS (PARTITION BY id)`) | ✅       |                                                                                                                                                                                                                                                                                                                                                                                      |
| إطار `ROWS`                                                             | ✅       |                                                                                                                                                                                                                                                                                                                                                                                      |
| إطار `RANGE`                                                            | ✅       | يُستخدم افتراضيًا عندما لا يُحدَّد الإطار صراحةً (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`).                                                                                                                                                                                                                                                                              |
| صياغة `INTERVAL` لإطار `DateTime` `RANGE OFFSET`                        | ❌       | حدِّد عدد الثواني بدلًا من ذلك (`RANGE` يعمل مع أي نوع رقمي).                                                                                                                                                                                                                                                                                                                        |
| إطار `GROUPS`                                                           | ❌       |                                                                                                                                                                                                                                                                                                                                                                                      |
| حساب الدالة التجميعية على إطار (`sum(value) OVER (ORDER BY time)`)      | ✅       | جميع الدوال التجميعية مدعومة.                                                                                                                                                                                                                                                                                                                                                        |
| `rank()`, `dense_rank()`/`denseRank()`, `row_number()`                  | ✅       |                                                                                                                                                                                                                                                                                                                                                                                      |
| `percent_rank()`/`percentRank()`                                        | ✅       | تحسب بكفاءة الترتيب النسبي لقيمة داخل التقسيم. وتستبدل حساب SQL اليدوي الأطول والأكثر كلفةً حسابيةً، والمعبَّر عنه بالشكل `ifNull((rank() OVER (PARTITION BY x ORDER BY y) - 1) / nullif(count(1) OVER (PARTITION BY x) - 1, 0), 0)`.                                                                                                                                              |
| `cume_dist()`                                                           | ✅       | تحسب التوزيع التراكمي لقيمة داخل مجموعة من القيم. وتُرجع النسبة المئوية للصفوف التي تكون قيمها أقل من قيمة الصف الحالي أو مساوية لها.                                                                                                                                                                                                                                                |
| `lag/lead(value, offset)`                                               | ✅       | يمكنك أيضًا استخدام أحد الحلول البديلة التالية:<br /> 1) `any(value) OVER (... ROWS BETWEEN <offset> PRECEDING AND <offset> PRECEDING)`، أو `FOLLOWING` بدلًا من `PRECEDING` مع `lead` <br /> 2) `lagInFrame/leadInFrame`، وهما مماثلتان لكنهما يراعيان إطار النافذة. للحصول على سلوك مطابق تمامًا لـ `lag/lead`، استخدم `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. |
| [`ntile(buckets)`](./ntile.md)                                          | ✅       | حدِّد النافذة، على سبيل المثال، بالشكل `(PARTITION BY x ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`.                                                                                                                                                                                                                                                       |

<div id="syntax">
  ## الصياغة
</div>

```text
aggregate_function (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([
  [PARTITION BY grouping_column]
  [ORDER BY sorting_column]
  [ROWS or RANGE expression_to_bound_rows_within_the_group]
])
```

* `PARTITION BY` - يحدّد كيفية تقسيم مجموعة النتائج إلى مجموعات.
* `ORDER BY` - يحدّد كيفية ترتيب الصفوف داخل المجموعة أثناء احتساب aggregate&#95;function.
* `ROWS or RANGE` - يحدّد حدود الإطار، ويُحتسب aggregate&#95;function ضمن هذا الإطار.
* `WINDOW` - يتيح لعدة تعبيرات استخدام تعريف النافذة نفسه.

```text
      PARTITION
┌─────────────────┐  <-- UNBOUNDED PRECEDING (BEGINNING of the PARTITION)
│                 │
│                 │
│=================│  <-- N PRECEDING  <─┐
│      N ROWS     │                     │  F
│  Before CURRENT │                     │  R
│~~~~~~~~~~~~~~~~~│  <-- CURRENT ROW    │  A
│     M ROWS      │                     │  M
│   After CURRENT │                     │  E
│=================│  <-- M FOLLOWING  <─┘
│                 │
│                 │
└─────────────────┘  <--- UNBOUNDED FOLLOWING (END of the PARTITION)
```

<div id="functions">
  ## الدوال التي لا يمكن استخدامها إلا كدوال النوافذ
</div>

لا يمكن استخدام الدوال التالية إلا كدوال النوافذ. ومعظمها دوال SQL قياسية، بينما تُعد `lagInFrame` و`leadInFrame` و`nonNegativeDerivative` امتدادات خاصة بـ ClickHouse.

| الدالة                                                                                                     | الوصف                                                                                                |
| ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [`row_number()`](./row_number.md)                                                                          | رقّم الصف الحالي داخل التقسيم الخاص به بدءًا من 1.                                                   |
| [`first_value(x)`](./first_value.md)                                                                       | أعد أول قيمة جرى تقييمها ضمن إطاره المرتّب.                                                          |
| [`last_value(x)`](./last_value.md)                                                                         | أعد آخر قيمة جرى تقييمها ضمن إطاره المرتّب.                                                          |
| [`nth_value(x, offset)`](./nth_value.md)                                                                   | أعد أول قيمة غير NULL جرى تقييمها عند الصف رقم n (offset) ضمن إطاره المرتّب.                         |
| [`rank()`](./rank.md)                                                                                      | رتّب الصف الحالي داخل التقسيم الخاص به مع وجود فجوات.                                                |
| [`dense_rank()`](./dense_rank.md)                                                                          | رتّب الصف الحالي داخل التقسيم الخاص به من دون فجوات.                                                 |
| [`lagInFrame(x)`](./lagInFrame.md)                                                                         | أعد قيمة جرى تقييمها عند صف يقع قبل الصف الحالي بإزاحة مادية محددة داخل الإطار المرتّب.              |
| [`leadInFrame(x)`](./leadInFrame.md)                                                                       | أعد قيمة جرى تقييمها عند صف يقع بعد الصف الحالي بعدد صفوف الإزاحة داخل الإطار المرتّب.               |
| [`ntile(buckets)`](./ntile.md)                                                                             | قسّم الصفوف المرتّبة داخل التقسيم إلى العدد المحدد من buckets، ثم أعد رقم bucket الخاص بالصف الحالي. |
| [`nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])`](./nonNegativeDerivative.md) | احسب المشتقة غير السالبة لـ `metric_column` بالنسبة إلى `timestamp_column`. وهي خاصة بـ ClickHouse.  |

<div id="examples">
  ## أمثلة
</div>

فيما يلي بعض الأمثلة على كيفية استخدام دوال النوافذ.

<div id="numbering-rows">
  ### ترقيم الصفوف
</div>

```sql
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M');
```

```sql
SELECT
    player,
    salary,
    row_number() OVER (ORDER BY salary ASC) AS row
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┐
│ Michael Stanley │ 150000 │   1 │
│ Scott Harrison  │ 150000 │   2 │
│ Charles Juarez  │ 190000 │   3 │
│ Gary Chen       │ 195000 │   4 │
│ Robert George   │ 195000 │   5 │
└─────────────────┴────────┴─────┘
```

```sql
SELECT
    player,
    salary,
    row_number() OVER (ORDER BY salary ASC) AS row,
    rank() OVER (ORDER BY salary ASC) AS rank,
    dense_rank() OVER (ORDER BY salary ASC) AS denseRank
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┬─rank─┬─denseRank─┐
│ Michael Stanley │ 150000 │   1 │    1 │         1 │
│ Scott Harrison  │ 150000 │   2 │    1 │         1 │
│ Charles Juarez  │ 190000 │   3 │    3 │         2 │
│ Gary Chen       │ 195000 │   4 │    4 │         3 │
│ Robert George   │ 195000 │   5 │    4 │         3 │
└─────────────────┴────────┴─────┴──────┴───────────┘
```

<div id="aggregation-functions">
  ### دوال التجميع
</div>

قارن راتب كل لاعب بمتوسط رواتب فريقه.

```sql
SELECT
    player,
    salary,
    team,
    avg(salary) OVER (PARTITION BY team) AS teamAvg,
    salary - teamAvg AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamAvg─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  170000 │  20000 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  170000 │ -20000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  180000 │ -30000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

قارن راتب كل لاعب بأقصى راتب في فريقه.

```sql
SELECT
    player,
    salary,
    team,
    max(salary) OVER (PARTITION BY team) AS teamMax,
    salary - teamMax AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamMax─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  190000 │      0 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  190000 │ -40000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  195000 │ -45000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

<div id="partitioning-by-column">
  ### التقسيم حسب العمود
</div>

```sql
CREATE TABLE wf_partition
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64    
)
ENGINE = Memory;

INSERT INTO wf_partition FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (2,0,0), (3,0,0);

SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values
FROM wf_partition
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3]      │   <┐   
│        1 │     2 │     2 │ [1,2,3]      │    │  1-st group
│        1 │     3 │     3 │ [1,2,3]      │   <┘ 
│        2 │     0 │     0 │ [0]          │   <- 2-nd group
│        3 │     0 │     0 │ [0]          │   <- 3-d group
└──────────┴───────┴───────┴──────────────┘
```

<div id="frame-bounding">
  ### ضبط حدود الإطار
</div>

```sql
CREATE TABLE wf_frame
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64
)
ENGINE = Memory;

INSERT INTO wf_frame FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (1,4,4), (1,5,5);
```

```sql
-- Frame is bounded by bounds of a partition (BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
    
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- short form - no bound expression, no order by,
-- an equalent of `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values_short,
    groupArray(value) OVER (PARTITION BY part_key
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values_short─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴────────────────────┴──────────────┘
```

```sql
-- frame is bounded by the beginning of a partition and the current row
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- short form (frame is bounded by the beginning of a partition and the current row)
-- an equalent of `ORDER BY order ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order ASC) AS frame_values_short,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values_short─┬─frame_values─┐
│        1 │     1 │     1 │ [1]                │ [1]          │
│        1 │     2 │     2 │ [1,2]              │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]            │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]          │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴────────────────────┴──────────────┘
```

```sql
-- frame is bounded by the beginning of a partition and the current row, but order is backward
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order DESC) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │
│        1 │     2 │     2 │ [5,4,3,2]    │
│        1 │     3 │     3 │ [5,4,3]      │
│        1 │     4 │     4 │ [5,4]        │
│        1 │     5 │     5 │ [5]          │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- sliding frame - 1 PRECEDING ROW AND CURRENT ROW
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [2,3]        │
│        1 │     4 │     4 │ [3,4]        │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- sliding frame - ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING 
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [2,3,4,5]    │
│        1 │     4 │     4 │ [3,4,5]      │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- row_number does not respect the frame, so rn_1 = rn_2 = rn_3 != rn_4
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER w1 AS frame_values,
    row_number() OVER w1 AS rn_1,
    sum(1) OVER w1 AS rn_2,
    row_number() OVER w2 AS rn_3,
    sum(1) OVER w2 AS rn_4
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order DESC),
    w2 AS (
        PARTITION BY part_key 
        ORDER BY order DESC 
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┬─rn_1─┬─rn_2─┬─rn_3─┬─rn_4─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │    5 │    5 │    5 │    2 │
│        1 │     2 │     2 │ [5,4,3,2]    │    4 │    4 │    4 │    2 │
│        1 │     3 │     3 │ [5,4,3]      │    3 │    3 │    3 │    2 │
│        1 │     4 │     4 │ [5,4]        │    2 │    2 │    2 │    2 │
│        1 │     5 │     5 │ [5]          │    1 │    1 │    1 │    1 │
└──────────┴───────┴───────┴──────────────┴──────┴──────┴──────┴──────┘
```

```sql
-- first_value and last_value respect the frame
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    first_value(value) OVER w1 AS first_value_1,
    last_value(value) OVER w1 AS last_value_1,
    groupArray(value) OVER w2 AS frame_values_2,
    first_value(value) OVER w2 AS first_value_2,
    last_value(value) OVER w2 AS last_value_2
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order ASC),
    w2 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─first_value_1─┬─last_value_1─┬─frame_values_2─┬─first_value_2─┬─last_value_2─┐
│ [1]            │             1 │            1 │ [1]            │             1 │            1 │
│ [1,2]          │             1 │            2 │ [1,2]          │             1 │            2 │
│ [1,2,3]        │             1 │            3 │ [2,3]          │             2 │            3 │
│ [1,2,3,4]      │             1 │            4 │ [3,4]          │             3 │            4 │
│ [1,2,3,4,5]    │             1 │            5 │ [4,5]          │             4 │            5 │
└────────────────┴───────────────┴──────────────┴────────────────┴───────────────┴──────────────┘
```

```sql
-- second value within the frame
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(value, 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─second_value─┐
│ [1]            │            0 │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

```sql
-- second value within the frame + Null for missing values
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(toNullable(value), 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─second_value─┐
│ [1]            │         ᴺᵁᴸᴸ │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

<div id="real-world-examples">
  ## أمثلة واقعية
</div>

تعالج الأمثلة التالية مشكلات واقعية شائعة.

<div id="maximumtotal-salary-per-department">
  ### أعلى راتب/إجمالي الرواتب لكل قسم
</div>

```sql
CREATE TABLE employees
(
    `department` String,
    `employee_name` String,
    `salary` Float
)
ENGINE = Memory;

INSERT INTO employees FORMAT Values
   ('Finance', 'Jonh', 200),
   ('Finance', 'Joan', 210),
   ('Finance', 'Jean', 505),
   ('IT', 'Tim', 200),
   ('IT', 'Anna', 300),
   ('IT', 'Elen', 500);
```

```sql
SELECT
    department,
    employee_name AS emp,
    salary,
    max_salary_per_dep,
    total_salary_per_dep,
    round((salary / total_salary_per_dep) * 100, 2) AS `share_per_dep(%)`
FROM
(
    SELECT
        department,
        employee_name,
        salary,
        max(salary) OVER wndw AS max_salary_per_dep,
        sum(salary) OVER wndw AS total_salary_per_dep
    FROM employees
    WINDOW wndw AS (
        PARTITION BY department
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
    ORDER BY
        department ASC,
        employee_name ASC
);

┌─department─┬─emp──┬─salary─┬─max_salary_per_dep─┬─total_salary_per_dep─┬─share_per_dep(%)─┐
│ Finance    │ Jean │    505 │                505 │                  915 │            55.19 │
│ Finance    │ Joan │    210 │                505 │                  915 │            22.95 │
│ Finance    │ Jonh │    200 │                505 │                  915 │            21.86 │
│ IT         │ Anna │    300 │                500 │                 1000 │               30 │
│ IT         │ Elen │    500 │                500 │                 1000 │               50 │
│ IT         │ Tim  │    200 │                500 │                 1000 │               20 │
└────────────┴──────┴────────┴────────────────────┴──────────────────────┴──────────────────┘
```

<div id="cumulative-sum">
  ### المجموع التراكمي
</div>

```sql
CREATE TABLE warehouse
(
    `item` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory

INSERT INTO warehouse VALUES
    ('sku38', '2020-01-01', 9),
    ('sku38', '2020-02-01', 1),
    ('sku38', '2020-03-01', -4),
    ('sku1', '2020-01-01', 1),
    ('sku1', '2020-02-01', 1),
    ('sku1', '2020-03-01', 1);
```

```sql
SELECT
    item,
    ts,
    value,
    sum(value) OVER (PARTITION BY item ORDER BY ts ASC) AS stock_balance
FROM warehouse
ORDER BY
    item ASC,
    ts ASC;

┌─item──┬──────────────────ts─┬─value─┬─stock_balance─┐
│ sku1  │ 2020-01-01 00:00:00 │     1 │             1 │
│ sku1  │ 2020-02-01 00:00:00 │     1 │             2 │
│ sku1  │ 2020-03-01 00:00:00 │     1 │             3 │
│ sku38 │ 2020-01-01 00:00:00 │     9 │             9 │
│ sku38 │ 2020-02-01 00:00:00 │     1 │            10 │
│ sku38 │ 2020-03-01 00:00:00 │    -4 │             6 │
└───────┴─────────────────────┴───────┴───────────────┘
```

<div id="moving--sliding-average-per-3-rows">
  ### المتوسط المتحرك / المتوسط المنزلق (لكل 3 صفوف)
</div>

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('cpu_temp', '2020-01-01 00:00:00', 87),
                          ('cpu_temp', '2020-01-01 00:00:01', 77),
                          ('cpu_temp', '2020-01-01 00:00:02', 93),
                          ('cpu_temp', '2020-01-01 00:00:03', 87),
                          ('cpu_temp', '2020-01-01 00:00:04', 87),
                          ('cpu_temp', '2020-01-01 00:00:05', 87),
                          ('cpu_temp', '2020-01-01 00:00:06', 87),
                          ('cpu_temp', '2020-01-01 00:00:07', 87);
```

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (
        PARTITION BY metric 
        ORDER BY ts ASC 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───┬──────────────────ts─┬─value─┬───moving_avg_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:01 │    77 │                82 │
│ cpu_temp │ 2020-01-01 00:00:02 │    93 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:03 │    87 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:04 │    87 │                89 │
│ cpu_temp │ 2020-01-01 00:00:05 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:06 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:07 │    87 │                87 │
└──────────┴─────────────────────┴───────┴───────────────────┘
```

<div id="moving--sliding-average-per-10-seconds">
  ### المتوسط المتحرك / المتوسط المنزلق (كل 10 ثوانٍ)
</div>

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (PARTITION BY metric ORDER BY ts
      RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS moving_avg_10_seconds_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;
    
┌─metric───┬──────────────────ts─┬─value─┬─moving_avg_10_seconds_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:01:10 │    77 │                         77 │
│ cpu_temp │ 2020-01-01 00:02:20 │    93 │                         93 │
│ cpu_temp │ 2020-01-01 00:03:30 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:04:40 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:05:50 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:06:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:07:10 │    87 │                         87 │
└──────────┴─────────────────────┴───────┴────────────────────────────┘
```

<div id="moving--sliding-average-per-10-days">
  ### المتوسط المتحرك / المتوسط المنزلق (كل 10 أيام)
</div>

تُخزَّن درجة الحرارة بدقة الثواني، لكن باستخدام `Range` و`ORDER BY toDate(ts)` نُنشئ إطارًا بحجم 10 وحدات، وبما أن `toDate(ts)` يُستخدم هنا، فإن الوحدة هي اليوم.

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('ambient_temp', '2020-01-01 00:00:00', 16),
                          ('ambient_temp', '2020-01-01 12:00:00', 16),
                          ('ambient_temp', '2020-01-02 11:00:00', 9),
                          ('ambient_temp', '2020-01-02 12:00:00', 9),                          
                          ('ambient_temp', '2020-02-01 10:00:00', 10),
                          ('ambient_temp', '2020-02-01 12:00:00', 10),
                          ('ambient_temp', '2020-02-10 12:00:00', 12),                          
                          ('ambient_temp', '2020-02-10 13:00:00', 12),
                          ('ambient_temp', '2020-02-20 12:00:01', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16);
```

```sql
SELECT
    metric,
    ts,
    value,
    round(avg(value) OVER (PARTITION BY metric ORDER BY toDate(ts) 
       RANGE BETWEEN 10 PRECEDING AND CURRENT ROW),2) AS moving_avg_10_days_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───────┬──────────────────ts─┬─value─┬─moving_avg_10_days_temp─┐
│ ambient_temp │ 2020-01-01 00:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-02 11:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-01-02 12:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-02-01 10:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-01 12:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-10 12:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-10 13:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-20 12:00:01 │    16 │                   13.33 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
└──────────────┴─────────────────────┴───────┴─────────────────────────┘
```

<div id="references">
  ## المراجع
</div>

<div id="github-issues">
  ### مشكلات GitHub
</div>

خارطة الطريق للدعم المبدئي لدوال النوافذ موجودة [في هذه المشكلة](https://github.com/ClickHouse/ClickHouse/issues/18097).

تحمل جميع مشكلات GitHub المتعلقة بدوال النوافذ الوسم [comp-window-functions](https://github.com/ClickHouse/ClickHouse/labels/comp-window-functions).

<div id="tests">
  ### الاختبارات
</div>

تحتوي هذه الاختبارات على أمثلة على البنية النحوية المدعومة حاليًا:

https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/window&#95;functions.xml

https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0&#95;stateless/01591&#95;window&#95;functions.sql

<div id="postgres-docs">
  ### وثائق Postgres
</div>

https://www.postgresql.org/docs/current/sql-select.html#SQL-WINDOW

https://www.postgresql.org/docs/devel/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS

https://www.postgresql.org/docs/devel/functions-window.html

https://www.postgresql.org/docs/devel/tutorial-window.html

<div id="mysql-docs">
  ### وثائق MySQL
</div>

https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html

<div id="related-content">
  ## محتوى مرتبط
</div>

* المدونة: [العمل مع بيانات السلاسل الزمنية في ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* المدونة: [دوال النوافذ والمصفوفات لتسلسلات commits في Git](https://clickhouse.com/blog/clickhouse-window-array-functions-git-commits)
* المدونة: [إدخال البيانات إلى ClickHouse - الجزء 3 - باستخدام S3](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)