---
description: 'توثيق لجملة WITH'
sidebar_label: 'WITH'
slug: /sql-reference/statements/select/with
title: 'جملة WITH'
doc_type: 'مرجع'
---

يدعم ClickHouse تعبيرات الجدول الشائعة ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL))، والتعبيرات القياسية الشائعة، والاستعلامات التكرارية.

<div id="common-table-expressions">
  ## تعبيرات الجدول الشائعة
</div>

تمثل تعبيرات الجدول الشائعة استعلامات فرعية مسماة.
ويمكن الإشارة إليها بالاسم في أي موضع داخل استعلام `SELECT` يُسمح فيه باستخدام تعبير جدول.
كما يمكن الإشارة إلى الاستعلامات الفرعية المسماة بالاسم ضمن نطاق الاستعلام الحالي أو ضمن نطاقات الاستعلامات الفرعية التابعة.

يُستبدل كل مرجع إلى تعبير جدول شائع في استعلامات `SELECT` دائمًا بالاستعلام الفرعي الوارد في تعريفه، ما لم يكن الـ CTE معرّفًا صراحةً على أنه materialized (راجع [تعبيرات الجدول الشائعة المادية](#materialized-common-table-expressions)).
ويُمنع الاستدعاء التكراري بإخفاء الـ CTE الحالي من عملية حلّ المعرّفات.

يرجى ملاحظة أن تعبيرات الجدول الشائعة لا تضمن النتائج نفسها في جميع المواضع التي يُشار إليها فيها، لأن الاستعلام يُعاد تنفيذه عند كل استخدام.

<div id="common-table-expressions-syntax">
  ### بناء الجملة
</div>

```sql
WITH <identifier> AS [MATERIALIZED] <subquery expression>
```

<div id="common-table-expressions-example">
  ### مثال
</div>

مثال على حالة يُعاد فيها تنفيذ الاستعلام الفرعي:

```sql
WITH cte_numbers AS
(
    SELECT
        num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT
    count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers)
```

لو كانت تعبيرات الجدول الشائعة تمرّر النتائج نفسها حرفيًا، لا مجرد جزء من الشيفرة، لرأيت دائمًا `1000000`

لكن بما أننا نشير إلى `cte_numbers` مرتين، تُولَّد أرقام عشوائية في كل مرة، ولذلك نرى نتائج عشوائية مختلفة مثل `280501, 392454, 261636, 196227` وهكذا...

<div id="materialized-common-table-expressions">
  ## تعبيرات الجدول الشائعة المُجسَّدة
</div>

يفترض ClickHouse افتراضيًا إدراج الاستعلام الفرعي الخاص بـ CTE في كل موضع يُشار إليه فيه، ثم يعيد تنفيذه في كل مرة.
تؤدي إضافة الكلمة المفتاحية `MATERIALIZED` إلى جعل ClickHouse ينفّذ الاستعلام الفرعي لـ CTE **مرة واحدة فقط**، ثم يخزّن النتائج في جدول مؤقت، ويستخدم هذا الجدول لتلبية جميع الإشارات إليه.
ويكون هذا مفيدًا بشكل خاص عندما يُشار إلى CTE نفسه عدة مرات داخل استعلام واحد (على سبيل المثال، في عمليات self-join أو في عدة استعلامات فرعية `IN`)، لأن العملية الحسابية الأساسية لا تحدث إلا مرة واحدة.

:::note
تُعد تعبيرات الجدول الشائعة المُجسَّدة ميزة **تجريبية**.
ويتطلب استخدامها تفعيل [المحلّل](/ar/operations/analyzer) والإعداد `enable_materialized_cte`.
:::

<div id="materialized-common-table-expressions-syntax">
  ### الصيغة
</div>

```sql
WITH <identifier> AS MATERIALIZED (<subquery>)
SELECT ...
```

<div id="materialized-cte-when-to-use">
  ### متى نستخدم تعبيرات الجدول الشائعة المُجسَّدة
</div>

تكون تعبيرات الجدول الشائعة المُجسَّدة أكثر فائدة في الحالات التالية:

* عند الرجوع إلى تعبير الجدول الشائع نفسه **أكثر من مرة** في query.
  فمن دون `MATERIALIZED`، يُعاد تنفيذ الاستعلام الفرعي بشكل مستقل مع كل مرجع.
* عندما يحتوي تعبير الجدول الشائع على دوال **غير حتمية** مثل `generateRandom`.
  ويضمن التجسيد أن ترى جميع المراجع البيانات نفسها.
* عندما يتضمن تعبير الجدول الشائع **حسابات مكلفة** (مثل التجميعات، وعمليات JOIN، وعمليات المسح الكبيرة) لا ينبغي تكرارها.

:::tip
إذا جرى الرجوع إلى تعبير جدول شائع مُجسَّد مرة واحدة فقط، فإن ClickHouse يضمّنه تلقائيًا من جديد كاستعلام فرعي عادي لتجنّب overhead غير الضروري.
:::

<div id="materialized-common-table-expressions-examples">
  ### أمثلة
</div>

**المثال 1:** ربط ذاتي لتعبير جدول شائع مُجسَّد

من دون `MATERIALIZED`، سينفِّذ كل جانب من عملية الربط الاستعلام الفرعي بشكل مستقل.
مع `MATERIALIZED`، يُمسَح الجدول مرة واحدة، ويقرأ جانبا الربط من الجدول المؤقت نفسه.

```sql
SET enable_materialized_cte = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE = Memory;
INSERT INTO users VALUES (1231, 'John', 33), (6666, 'Ksenia', 48), (8888, 'Alice', 50);

WITH
    a AS MATERIALIZED (SELECT * FROM users WHERE name = 'Alice')
SELECT count() FROM a AS l JOIN a AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       1 │
└─────────┘
```

**مثال 2:** نتائج حتمية مع دوال غير حتمية

تنتج تعبيرات الجدول الشائع العادية التي تستخدم `generateRandom` نتائج مختلفة في كل مرة تتم الإشارة إليها.
ويضمن تجسيد تعبير الجدول الشائع اتساق النتائج:

```sql
SET enable_materialized_cte = 1;

WITH cte_numbers AS MATERIALIZED
(
    SELECT num
    FROM generateRandom('num UInt64', NULL)
    LIMIT 1000000
)
SELECT count()
FROM cte_numbers
WHERE num IN (SELECT num FROM cte_numbers);
```

نظرًا لأن كلا المرجعين يقرآن من البيانات المُجسَّدة نفسها، تكون النتيجة دائمًا `1000000`.

**مثال 3:** ربط تعبيرات الجدول الشائعة المُجسَّدة

يمكن أن تشير تعبيرات الجدول الشائعة المُجسَّدة إلى تعبيرات جدول شائعة مُجسَّدة أخرى.
يحلّ ClickHouse التبعيات ويُجسِّدها بالترتيب الصحيح:

```sql
SET enable_materialized_cte = 1;

WITH
    a AS MATERIALIZED (SELECT uid, name FROM users),
    b AS MATERIALIZED (SELECT uid FROM a)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

لا يهم ترتيب تعريفات تعبيرات الجدول الشائعة — إذ يُسمح بالإشارة إلى التعريفات اللاحقة:

```sql
SET enable_materialized_cte = 1;

WITH
    b AS MATERIALIZED (SELECT uid FROM a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b AS l LEFT SEMI JOIN b AS r ON l.uid = r.uid;
```

```response
┌─count()─┐
│       3 │
└─────────┘
```

<div id="materialized-cte-restrictions">
  ### القيود
</div>

* **يتطلب إعدادًا تجريبيًا**: يجب تفعيل الإعداد `enable_materialized_cte`.
* **المحلِّل مطلوب**: لا تعمل تعبيرات الجدول الشائعة المُجسَّدة إلا عند تفعيل [المحلِّل](/ar/operations/analyzer) (`enable_analyzer = 1`).
* **غير مدعوم مع `RECURSIVE`**: لا يُسمح بدمج الكلمتين المفتاحيتين `MATERIALIZED` و`RECURSIVE`، وينتج عن ذلك الاستثناء `UNSUPPORTED_METHOD`.
* **تعبيرات الجدول الشائعة المرتبطة محظورة**: لا يمكن لتعبير جدول شائع مُجسَّد الإشارة إلى أعمدة من النطاقات الخارجية للاستعلام.

<div id="common-scalar-expressions">
  ## التعبيرات القياسية الشائعة
</div>

يتيح لك ClickHouse تعريف أسماء مستعارة لأي تعبيرات قياسية ضمن عبارة `WITH`.
ويمكن الإشارة إلى التعبيرات القياسية الشائعة في أي موضع من الاستعلام.

:::note
إذا كان التعبير القياسي الشائع يشير إلى شيء غير قيمة حرفية ثابتة، فقد يؤدي ذلك إلى وجود [متغيرات حرة](https://en.wikipedia.org/wiki/Free_variables_and_bound_variables).
يحلّ ClickHouse أي معرّف ضمن أقرب نطاق ممكن، ما يعني أن المتغيرات الحرة قد تشير إلى كيانات غير متوقعة عند تعارض الأسماء، أو قد تؤدي إلى استعلام فرعي مترابط.
يُوصى بتعريف CSE على هيئة [دالة لامبدا](/ar/sql-reference/functions/overview#arrow-operator-and-lambda) (وهذا ممكن فقط عند تمكين [المحلّل](/ar/operations/analyzer))، مع ربط جميع المعرّفات المستخدمة، للحصول على سلوك أكثر قابلية للتنبؤ عند حلّ معرّفات التعبير.
:::

<div id="common-scalar-expressions-syntax">
  ### الصياغة
</div>

```sql
WITH <expression> AS <identifier>
```

<div id="materialized-common-table-expressions-examples">
  ### أمثلة
</div>

**المثال 1:** استخدام تعبير ثابت كـ&quot;متغير&quot;

```sql
WITH '2019-08-01 15:23:00' AS ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound;
```

**مثال 2:** استخدام الدوال عالية الرتبة لحصر المعرّفات

```sql
WITH
    '.txt' as extension,
    (id, extension) -> concat(lower(id), extension) AS gen_name
SELECT gen_name('test', '.sql') as file_name;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**مثال 3:** استخدام الدوال عالية الرتبة مع المتغيرات الحرة

تُظهر استعلامات المثال التالية أن المعرّفات غير المرتبطة تُفسَّر على أنها كيان ضمن أقرب نطاق.
هنا، لا يكون `extension` مرتبطًا داخل جسم دالة لامبدا `gen_name`.
وعلى الرغم من أن `extension` مُعرّف بالقيمة `'.txt'` كتعبير قياسي مشترك ضمن نطاق تعريف `generated_names` واستخدامه، فإنه يُفسَّر على أنه عمود في الجدول `extension_list`، لأنه متاح في الاستعلام الفرعي `generated_names`.

```sql
CREATE TABLE extension_list
(
    extension String
)
ORDER BY extension
AS SELECT '.sql';

WITH
    '.txt' as extension,
    generated_names as (
        WITH
            (id) -> concat(lower(id), extension) AS gen_name
        SELECT gen_name('test') as file_name FROM extension_list
    )
SELECT file_name FROM generated_names;
```

```response
   ┌─file_name─┐
1. │ test.sql  │
   └───────────┘
```

**مثال 4:** استبعاد نتيجة التعبير sum(bytes) من قائمة أعمدة عبارة SELECT

```sql
WITH sum(bytes) AS s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s;
```

**مثال 5:** استخدام نتائج استعلام فرعي قيَمي

```sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10;
```

**مثال 6:** إعادة استخدام تعبير في استعلام فرعي

```sql
WITH test1 AS (SELECT i + 1, j + 1 FROM test1)
SELECT * FROM test1;
```

<div id="recursive-queries">
  ## الاستعلامات التكرارية
</div>

يتيح المُعدِّل الاختياري `RECURSIVE` لاستعلام `WITH` الإشارة إلى ناتجه الخاص. مثال:

**مثال:** جمع الأعداد الصحيحة من 1 إلى 100

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table WHERE number < 100
)
SELECT sum(number) FROM test_table;
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

:::note
تعتمد تعبيرات الجدول الشائعة التكرارية على [محلل الاستعلامات](/ar/operations/analyzer) الذي طُرح في الإصدار **`24.3`**. إذا كنت تستخدم الإصدار **`24.3+`** وواجهت استثناء **`(UNKNOWN_TABLE)`** أو **`(UNSUPPORTED_METHOD)`**، فهذا يشير إلى أن المحلل معطّل على المثيل أو الدور أو ملف التعريف لديك. لتفعيل المحلل، فعِّل الإعداد **`allow_experimental_analyzer`** أو حدِّث إعداد **`compatibility`** إلى إصدار أحدث.
اعتبارًا من الإصدار `24.8`، أصبح المحلل معتمدًا بالكامل لبيئة الإنتاج، وأُعيدت تسمية الإعداد `allow_experimental_analyzer` إلى `enable_analyzer`.
:::

تكون الصيغة العامة لاستعلام `WITH` التكراري دائمًا على النحو التالي: حد غير تكراري، ثم `UNION ALL`، ثم حد تكراري، بحيث لا يمكن إلا للحد التكراري أن يتضمن مرجعًا إلى مخرجات الاستعلام نفسه. ويُنفَّذ استعلام تعبير الجدول الشائع التكراري كما يلي:

1. قيِّم الحد غير التكراري. وضع نتيجة استعلام الحد غير التكراري في جدول عمل مؤقت.
2. ما دام جدول العمل غير فارغ، كرِّر الخطوات التالية:
   1. قيِّم الحد التكراري، مع استبدال المرجع الذاتي التكراري بالمحتويات الحالية لجدول العمل. وضع نتيجة استعلام الحد التكراري في جدول وسيط مؤقت.
   2. استبدل محتويات جدول العمل بمحتويات الجدول الوسيط، ثم أفرغ الجدول الوسيط.

تُستخدم الاستعلامات التكرارية عادةً للتعامل مع البيانات الهرمية أو البيانات ذات البنية الشجرية. على سبيل المثال، يمكننا كتابة استعلام يُجري اجتيازًا للشجرة:

**مثال:** اجتياز الشجرة

لننشئ أولًا جدول الشجرة:

```sql
DROP TABLE IF EXISTS tree;
CREATE TABLE tree
(
    id UInt64,
    parent_id Nullable(UInt64),
    data String
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tree VALUES (0, NULL, 'ROOT'), (1, 0, 'Child_1'), (2, 0, 'Child_2'), (3, 1, 'Child_1_1');
```

يمكننا اجتياز تلك الشجرة باستخدام الاستعلام التالي:

**مثال:** اجتياز الشجرة

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree;
```

```text
┌─id─┬─parent_id─┬─data──────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │
│  1 │         0 │ Child_1   │
│  2 │         0 │ Child_2   │
│  3 │         1 │ Child_1_1 │
└────┴───────────┴───────────┘
```

<div id="search-order">
  ### ترتيب البحث
</div>

لإنشاء ترتيب وفق أسلوب العمق أولًا، نحسب لكل صف في النتيجة مصفوفةً من الصفوف التي سبق أن زرناها:

**مثال:** ترتيب اجتياز الشجرة بأسلوب العمق أولًا

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id])
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY path;
```

```text
┌─id─┬─parent_id─┬─data──────┬─path────┐
│  0 │      ᴺᵁᴸᴸ │ ROOT      │ [0]     │
│  1 │         0 │ Child_1   │ [0,1]   │
│  3 │         1 │ Child_1_1 │ [0,1,3] │
│  2 │         0 │ Child_2   │ [0,2]   │
└────┴───────────┴───────────┴─────────┘
```

لإنشاء ترتيب الاجتياز بالعرض أولًا، يتمثل النهج القياسي في إضافة عمود يتتبّع عمق البحث:

**مثال:** اجتياز الشجرة بترتيب العرض أولًا

```sql
WITH RECURSIVE search_tree AS (
    SELECT id, parent_id, data, [t.id] AS path, toUInt64(0) AS depth
    FROM tree t
    WHERE t.id = 0
UNION ALL
    SELECT t.id, t.parent_id, t.data, arrayConcat(path, [t.id]), depth + 1
    FROM tree t, search_tree st
    WHERE t.parent_id = st.id
)
SELECT * FROM search_tree ORDER BY depth;
```

```text
┌─id─┬─link─┬─data──────┬─path────┬─depth─┐
│  0 │ ᴺᵁᴸᴸ │ ROOT      │ [0]     │     0 │
│  1 │    0 │ Child_1   │ [0,1]   │     1 │
│  2 │    0 │ Child_2   │ [0,2]   │     1 │
│  3 │    1 │ Child_1_1 │ [0,1,3] │     2 │
└────┴──────┴───────────┴─────────┴───────┘
```

<div id="cycle-detection">
  ### اكتشاف الحلقات
</div>

لنُنشئ أولًا جدولًا لتمثيل الرسم البياني:

```sql
DROP TABLE IF EXISTS graph;
CREATE TABLE graph
(
    from UInt64,
    to UInt64,
    label String
) ENGINE = MergeTree ORDER BY (from, to);

INSERT INTO graph VALUES (1, 2, '1 -> 2'), (1, 3, '1 -> 3'), (2, 3, '2 -> 3'), (1, 4, '1 -> 4'), (4, 5, '4 -> 5');
```

يمكننا اجتياز ذلك الرسم البياني باستعلام مثل هذا:

**مثال:** اجتياز الرسم البياني من دون اكتشاف الدورات

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
    UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┐
│    1 │  4 │ 1 -> 4 │
│    1 │  2 │ 1 -> 2 │
│    1 │  3 │ 1 -> 3 │
│    2 │  3 │ 2 -> 3 │
│    4 │  5 │ 4 -> 5 │
└──────┴────┴────────┘
```

ولكن إذا أضفنا دورة إلى ذلك الرسم البياني، فسيفشل الاستعلام السابق مع ظهور الخطأ `Maximum recursive CTE evaluation depth`:

```sql
INSERT INTO graph VALUES (5, 1, '5 -> 1');

WITH RECURSIVE search_graph AS (
    SELECT from, to, label FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label
    FROM graph g, search_graph sg
    WHERE g.from = sg.to
)
SELECT DISTINCT * FROM search_graph ORDER BY from;
```

```text
Code: 306. DB::Exception: Received from localhost:9000. DB::Exception: Maximum recursive CTE evaluation depth (1000) exceeded, during evaluation of search_graph AS (SELECT from, to, label FROM graph AS g UNION ALL SELECT g.from, g.to, g.label FROM graph AS g, search_graph AS sg WHERE g.from = sg.to). Consider raising max_recursive_cte_evaluation_depth setting.: While executing RecursiveCTESource. (TOO_DEEP_RECURSION)
```

الطريقة القياسية للتعامل مع الحلقات هي حساب مصفوفة بالعُقد التي سبقَت زيارتها:

**مثال:** اجتياز الرسم البياني مع اكتشاف الحلقات

```sql
WITH RECURSIVE search_graph AS (
    SELECT from, to, label, false AS is_cycle, [tuple(g.from, g.to)] AS path FROM graph g
UNION ALL
    SELECT g.from, g.to, g.label, has(path, tuple(g.from, g.to)), arrayConcat(sg.path, [tuple(g.from, g.to)])
    FROM graph g, search_graph sg
    WHERE g.from = sg.to AND NOT is_cycle
)
SELECT * FROM search_graph WHERE is_cycle ORDER BY from;
```

```text
┌─from─┬─to─┬─label──┬─is_cycle─┬─path──────────────────────┐
│    1 │  4 │ 1 -> 4 │ true     │ [(1,4),(4,5),(5,1),(1,4)] │
│    4 │  5 │ 4 -> 5 │ true     │ [(4,5),(5,1),(1,4),(4,5)] │
│    5 │  1 │ 5 -> 1 │ true     │ [(5,1),(1,4),(4,5),(5,1)] │
└──────┴────┴────────┴──────────┴───────────────────────────┘
```

<div id="infinite-queries">
  ### استعلامات لا نهائية
</div>

يمكن أيضًا استخدام استعلامات تعبير الجدول الشائع التكرارية اللانهائية عند استخدام `LIMIT` في الاستعلام الخارجي:

**مثال:** استعلام تكراري لا نهائي لتعبير الجدول الشائع

```sql
WITH RECURSIVE test_table AS (
    SELECT 1 AS number
UNION ALL
    SELECT number + 1 FROM test_table
)
SELECT sum(number) FROM (SELECT number FROM test_table LIMIT 100);
```

```text
┌─sum(number)─┐
│        5050 │
└─────────────┘
```

<div id="trailing-comma">
  ## الفاصلة الختامية
</div>

يُسمح بوضع فاصلة بعد آخر عنصر في عبارة `WITH`:

```sql
WITH
    (SELECT sum(number) FROM numbers(10)) AS total,
    total * 2 AS doubled,
SELECT total, doubled;
```