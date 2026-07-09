---
description: 'توثيق EXPLAIN'
sidebar_label: 'EXPLAIN'
sidebar_position: 39
slug: /sql-reference/statements/explain
title: 'عبارة EXPLAIN'
doc_type: 'reference'
---

يعرض خطة تنفيذ العبارة.

<div class="vimeo-container">
  <iframe
    src="//www.youtube.com/embed/hP6G2Nlz_cA"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
fullscreen;
picture-in-picture"
    allowfullscreen
  />
</div>

الصيغة:

```sql
EXPLAIN [AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE | TABLE OVERRIDE | WHATIF] [setting = value, ...]
    [
      SELECT ... |
      tableFunction(...) [COLUMNS (...)] [ORDER BY ...] [PARTITION BY ...] [PRIMARY KEY] [SAMPLE BY ...] [TTL ...]
    ]
    [FORMAT ...]
```

مثال:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) UNION ALL SELECT sum(number) FROM numbers(10) ORDER BY sum(number) ASC FORMAT TSV;
```

```sql
Output: sum(number)

Union
├──Aggregating
│  │  Keys:
│  │  Aggregates: sum(number)
│  │  Skip merging: 0
│  └──ReadFromSystemNumbers
│        Output: number
└──Sorting (Sorting for ORDER BY)
   │  Sort description: sum(number) ASC
   └──Aggregating
      │  Keys:
      │  Aggregates: sum(number)
      │  Skip merging: 0
      └──ReadFromSystemNumbers
            Output: number
```

<div id="explain-types">
  ## أنواع EXPLAIN
</div>

* `AST` — شجرة البنية المجرّدة.
* `SYNTAX` — نص الاستعلام بعد التحسينات على مستوى شجرة البنية المجرّدة.
* `QUERY TREE` — شجرة الاستعلام بعد التحسينات على مستوى شجرة الاستعلام.
* `PLAN` — خطة تنفيذ الاستعلام.
* `PIPELINE` — خط أنابيب تنفيذ الاستعلام.

<div id="explain-ast">
  ### EXPLAIN AST
</div>

يعرض AST للاستعلام. ويدعم جميع أنواع الاستعلامات، وليس `SELECT` فقط.

الإعدادات:

* `graph` – يعرض AST كرسم بياني موصوف بلغة وصف الرسوم البيانية [DOT](https://en.wikipedia.org/wiki/DOT_\(graph_description_language\)). القيمة الافتراضية: 0.

أمثلة:

```sql
EXPLAIN AST SELECT 1;
```

```sql
SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 1)
   ExpressionList (children 1)
    Literal UInt64_1
```

```sql
EXPLAIN AST ALTER TABLE t1 DELETE WHERE date = today();
```

```sql
  explain
  AlterQuery  t1 (children 1)
   ExpressionList (children 1)
    AlterCommand 27 (children 1)
     Function equals (children 1)
      ExpressionList (children 2)
       Identifier date
       Function today (children 1)
        ExpressionList
```

<div id="explain-syntax">
  ### EXPLAIN SYNTAX
</div>

يعرض شجرة البنية المجرّدة (AST) للاستعلام بعد تحليل البنية.

يتم ذلك من خلال تحليل الاستعلام، وبناء AST للاستعلام وشجرة الاستعلام، وتشغيل محلّل الاستعلام وتمريرات التحسين اختياريًا، ثم تحويل شجرة الاستعلام مرة أخرى إلى AST للاستعلام.

الإعدادات:

* `oneline` – اطبع الاستعلام في سطر واحد. القيمة الافتراضية: `0`.
* `run_query_tree_passes` – شغّل تمريرات شجرة الاستعلام قبل تفريغ شجرة الاستعلام. القيمة الافتراضية: `0`.
* `query_tree_passes` – إذا تم تعيين `run_query_tree_passes`، فسيحدّد عدد التمريرات المطلوب تشغيلها. ومن دون تحديد `query_tree_passes`، تُشغَّل جميع التمريرات.

أمثلة:

```sql title="Query"
EXPLAIN SYNTAX SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number;
```

```sql title="Response"
SELECT *
FROM system.numbers AS a, system.numbers AS b, system.numbers AS c
WHERE (a.number = b.number) AND (b.number = c.number)
```

باستخدام `run_query_tree_passes`:

```sql title="Query"
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number;
```

```sql title="Response"
SELECT
    __table1.number AS `a.number`,
    __table2.number AS `b.number`,
    __table3.number AS `c.number`
FROM system.numbers AS __table1
ALL INNER JOIN system.numbers AS __table2 ON __table1.number = __table2.number
ALL INNER JOIN system.numbers AS __table3 ON __table2.number = __table3.number
```

<div id="explain-query-tree">
  ### EXPLAIN QUERY TREE
</div>

الإعدادات:

* `run_passes` — شغّل جميع تمريرات شجرة الاستعلام قبل إخراج شجرة الاستعلام. القيمة الافتراضية: `1`.
* `dump_passes` — اعرض معلومات عن التمريرات المستخدمة قبل إخراج شجرة الاستعلام. القيمة الافتراضية: `0`.
* `passes` — يحدّد عدد التمريرات المطلوب تشغيلها. إذا ضُبطت على `-1`، فسيُشغِّل جميع التمريرات. القيمة الافتراضية: `-1`.
* `dump_tree` — اعرض شجرة الاستعلام. القيمة الافتراضية: `1`.
* `dump_ast` — اعرض AST للاستعلام المُولَّدة من شجرة الاستعلام. القيمة الافتراضية: `0`.

مثال:

```sql
EXPLAIN QUERY TREE SELECT id, value FROM test_table;
```

```sql
QUERY id: 0
  PROJECTION COLUMNS
    id UInt64
    value String
  PROJECTION
    LIST id: 1, nodes: 2
      COLUMN id: 2, column_name: id, result_type: UInt64, source_id: 3
      COLUMN id: 4, column_name: value, result_type: String, source_id: 3
  JOIN TREE
    TABLE id: 3, table_name: default.test_table
```

<div id="explain-plan">
  ### EXPLAIN PLAN
</div>

يعرض خطوات خطة الاستعلام.

الإعدادات:

* `optimize` — يتحكم في ما إذا كانت تحسينات خطة الاستعلام تُطبَّق قبل عرض الخطة. القيمة الافتراضية: 1.
* `header` — يطبع ترويسة المخرجات للخطوة. القيمة الافتراضية: 0.
* `description` — يطبع وصف الخطوة. القيمة الافتراضية: 1.
* `indexes` — يعرض الـ indexes المستخدمة، وعدد الأجزاء التي تمت تصفيتها، وعدد الحبيبات التي تمت تصفيتها لكل index مُطبَّق. القيمة الافتراضية: 0. مدعوم لجداول [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). بدءًا من ClickHouse &gt;= v25.9، لا يعرض هذا statement مخرجات مناسبة إلا عند استخدامه مع `SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0`.
* `projections` — يعرض جميع projections التي تم تحليلها وتأثيرها في التصفية على مستوى الأجزاء استنادًا إلى شروط primary key الخاصة بكل projection. ولكل projection، يتضمن هذا القسم إحصاءات مثل عدد الأجزاء والصفوف والعلامات والنطاقات التي جرى تقييمها باستخدام primary key الخاصة بها. كما يوضّح عدد data parts التي تم تخطيها بسبب هذه التصفية، من دون القراءة من projection نفسها. ويمكن تحديد ما إذا كانت projection قد استُخدمت فعليًا للقراءة أو جرى تحليلها فقط لأغراض التصفية من خلال الحقل `description`. القيمة الافتراضية: 0. مدعوم لجداول [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
* `actions` — يطبع معلومات تفصيلية عن actions الخاصة بالخطوة. القيمة الافتراضية: 1.
* `sorting` — يطبع وصف الفرز لكل خطوة في الخطة تنتج مخرجات مرتبة. القيمة الافتراضية: 0.
* `keep_logical_steps` — يُبقي خطوات الخطة المنطقية لعمليات الربط بدلًا من تحويلها إلى تطبيقات الربط الفعلية. القيمة الافتراضية: 0.
* `json` — يطبع خطوات خطة الاستعلام كسطر بتنسيق [JSON](/ar/interfaces/formats/JSON). القيمة الافتراضية: 0. يُنصح باستخدام تنسيق [TabSeparatedRaw (TSVRaw)](/ar/interfaces/formats/TabSeparatedRaw) لتجنّب إفلات غير ضروري.
* `input_headers` — يطبع ترويسات الإدخال للخطوة. القيمة الافتراضية: 0. ويكون مفيدًا غالبًا للمطورين فقط من أجل Debug المشكلات المتعلقة بعدم تطابق ترويسات الإدخال والإخراج.
* `column_structure` — يطبع أيضًا بنية columns في headers بالإضافة إلى الاسم والنوع. القيمة الافتراضية: 0. ويكون مفيدًا غالبًا للمطورين فقط من أجل Debug المشكلات المتعلقة بعدم تطابق ترويسات الإدخال والإخراج.
* `distributed` — يعرض query plans المُنفَّذة على العقد البعيدة للجداول الموزعة أو parallel replicas. غير مدعوم مع `json`. القيمة الافتراضية: 0.
* `compact` — عند تفعيله، يُخفي خطوات expression ومعلومات actions التفصيلية (المدخلات، والدوال، وAliases، ومواضع المخرجات) من الخطة. ولا يكون له تأثير إلا عندما `actions = 1`. القيمة الافتراضية: 1.
* `pretty` — يطبع شجرة الخطة باستخدام محارف رسم الخطوط (├──, └──, │) بدلًا من المسافات البادئة لعرض التسلسل الهرمي. كما ينسّق properties خطوة الربط ضمن السطر نفسه. القيمة الافتراضية: 1.

:::note
افتراضيًا، تكون `explain_query_plan_default = 'pretty'`، لذا تتم تهيئة `actions` و`compact` و`pretty` إلى `1`، وتُعرض الخطة بصيغة compact وpretty ومشروحة بـ action. إن تحديد أي من هذه الخيارات صراحةً في statement `EXPLAIN` (على سبيل المثال، `EXPLAIN actions = 0, compact = 0, pretty = 0 SELECT ...`) يتجاوز دائمًا القيمة الافتراضية.

قبل ClickHouse 26.7، كانت القيم الافتراضية لـ `actions` و`compact` و`pretty` هي `0`. ولا يزال بإمكانك الحصول على هذا الناتج عبر ضبط `explain_query_plan_default = 'legacy'` (عمومًا أو في `SETTINGS` الخاصة بكل query)، أو عبر ضبط `compatibility` على أي إصدار أقدم من `26.7`.

لا يفعّل الخياران `json` و`distributed` الإعدادات الافتراضية لـ `pretty` (`actions` و`compact` و`pretty`) حتى عندما تكون `explain_query_plan_default = 'pretty'`. ولتضمين تفاصيل action في مخرجاتهما، اضبط `actions = 1` يدويًا.
:::

مثال:

```sql
EXPLAIN SELECT sum(number) FROM numbers(10) GROUP BY number % 4  LIMIT 1;
```

```sql
Output: sum(number)

Limit (preliminary LIMIT)
│  Limit 1
│  Offset 0
└──Aggregating
   │  Keys: number MOD 4
   │  Aggregates: sum(number)
   │  Skip merging: 0
   └──ReadFromSystemNumbers
         Output: number
```

:::note
لا تتوفر إمكانية تقدير تكلفة الخطوات والاستعلامات.
:::

عندما تكون قيمة `json = 1`، تُمثَّل خطة الاستعلام بتنسيق JSON. تكون كل عقدة قاموسًا يحتوي دائمًا على المفاتيح `Node Type` و`Node Id` و`Plans`. تكون `Node Type` سلسلة نصية تحتوي على اسم الخطوة، وتكون `Node Id` معرّفًا فريدًا للخطوة (اسم الخطوة مع لاحقة رقمية، مثل `Union_10`). أما `Plans` فهي مصفوفة تحتوي على أوصاف الخطوات الفرعية. وقد تُضاف مفاتيح اختيارية أخرى حسب نوع العقدة والإعدادات.

مثال:

```sql
EXPLAIN json = 1, description = 0 SELECT 1 UNION ALL SELECT 2 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Union",
      "Node Id": "Union_10",
      "Plans": [
        {
          "Node Type": "Expression",
          "Node Id": "Expression_13",
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Node Id": "ReadFromStorage_0"
            }
          ]
        },
        {
          "Node Type": "Expression",
          "Node Id": "Expression_16",
          "Plans": [
            {
              "Node Type": "ReadFromStorage",
              "Node Id": "ReadFromStorage_4"
            }
          ]
        }
      ]
    }
  }
]
```

عند ضبط `description` = 1، يُضاف المفتاح `Description` إلى الخطوة:

```json
{
  "Node Type": "ReadFromStorage",
  "Description": "SystemOne"
}
```

مع `header` = 1، يُضاف المفتاح `Header` إلى الخطوة على شكل مصفوفة من الأعمدة.

مثال:

```sql
EXPLAIN json = 1, description = 0, header = 1 SELECT 1, 2 + dummy;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Node Id": "Expression_5",
      "Header": [
        {
          "Name": "1",
          "Type": "UInt8"
        },
        {
          "Name": "plus(2, dummy)",
          "Type": "UInt16"
        }
      ],
      "Plans": [
        {
          "Node Type": "ReadFromStorage",
          "Node Id": "ReadFromStorage_0",
          "Header": [
            {
              "Name": "dummy",
              "Type": "UInt8"
            }
          ]
        }
      ]
    }
  }
]
```

عند تعيين `indexes` = 1، يُضاف المفتاح `Indexes`. ويحتوي على مصفوفة من الفهارس المستخدمة. يُوصَف كل فهرس بصيغة JSON باستخدام المفتاح `Type` (سلسلة نصية `Partition Min-Max` أو `Partition` أو `Statistics` أو `PrimaryKey` أو `Skip`) ومفاتيح اختيارية:

* `Name` — اسم الفهرس (يُستخدم حاليًا فقط لفهارس `Skip`).
* `Keys` — مصفوفة الأعمدة التي يستخدمها الفهرس.
* `Condition` — الشرط المستخدم.
* `Description` — وصف الفهرس (يُستخدم حاليًا فقط لفهارس `Skip`).
* `Parts` — عدد الأجزاء بعد/قبل تطبيق الفهرس.
* `Granules` — عدد الحبيبات بعد/قبل تطبيق الفهرس.
* `Ranges` — عدد نطاقات الحبيبات بعد تطبيق الفهرس.

مثال:

```json
"Node Type": "ReadFromMergeTree",
"Indexes": [
  {
    "Type": "Partition Min-Max",
    "Keys": ["y"],
    "Condition": "(y in [1, +inf))",
    "Parts": 4/5,
    "Granules": 11/12
  },
  {
    "Type": "Partition",
    "Keys": ["y", "bitAnd(z, 3)"],
    "Condition": "and((bitAnd(z, 3) not in [1, 1]), and((y in [1, +inf)), (bitAnd(z, 3) not in [1, 1])))",
    "Parts": 3/4,
    "Granules": 10/11
  },
  {
    "Type": "PrimaryKey",
    "Keys": ["x", "y"],
    "Condition": "and((x in [11, +inf)), (y in [1, +inf)))",
    "Parts": 2/3,
    "Granules": 6/10,
    "Search Algorithm": "generic exclusion search"
  },
  {
    "Type": "Skip",
    "Name": "t_minmax",
    "Description": "minmax GRANULARITY 2",
    "Parts": 1/2,
    "Granules": 2/6
  },
  {
    "Type": "Skip",
    "Name": "t_set",
    "Description": "set GRANULARITY 2",
    "": 1/1,
    "Granules": 1/2
  }
]
```

عند تعيين `projections` = 1، يُضاف المفتاح `Projections`، ويحتوي على مصفوفة من الإسقاطات المحللة. يُوصف كل إسقاط بصيغة JSON بالمفاتيح التالية:

* `Name` — اسم الإسقاط.
* `Condition` — شرط المفتاح الأساسي المستخدم للإسقاط.
* `Description` — وصف لكيفية استخدام الإسقاط (على سبيل المثال، التصفية على مستوى الجزء).
* `Selected Parts` — عدد الأجزاء التي حدّدها الإسقاط.
* `Selected Marks` — عدد العلامات المحددة.
* `Selected Ranges` — عدد النطاقات المحددة.
* `Selected Rows` — عدد الصفوف المحددة.
* `Filtered Parts` — عدد الأجزاء التي تم تخطيها بسبب التصفية على مستوى الجزء.

مثال:

```json
"Node Type": "ReadFromMergeTree",
"Projections": [
  {
    "Name": "region_proj",
    "Description": "Projection has been analyzed and is used for part-level filtering",
    "Condition": "(region in ['us_west', 'us_west'])",
    "Search Algorithm": "binary search",
    "Selected Parts": 3,
    "Selected Marks": 3,
    "Selected Ranges": 3,
    "Selected Rows": 3,
    "Filtered Parts": 2
  },
  {
    "Name": "user_id_proj",
    "Description": "Projection has been analyzed and is used for part-level filtering",
    "Condition": "(user_id in [107, 107])",
    "Search Algorithm": "binary search",
    "Selected Parts": 1,
    "Selected Marks": 1,
    "Selected Ranges": 1,
    "Selected Rows": 1,
    "Filtered Parts": 2
  }
]
```

مع `actions` = 1، تعتمد المفاتيح المُضافة على نوع الخطوة.

مثال:

```sql
EXPLAIN json = 1, actions = 1, description = 0 SELECT 1 FORMAT TSVRaw;
```

```json
[
  {
    "Plan": {
      "Node Type": "Expression",
      "Node Id": "Expression_5",
      "Expression": {
        "Inputs": [
          {
            "Name": "dummy",
            "Type": "UInt8"
          }
        ],
        "Actions": [
          {
            "Node Type": "INPUT",
            "Result Type": "UInt8",
            "Result Name": "dummy",
            "Arguments": [0],
            "Removed Arguments": [0],
            "Result": 0
          },
          {
            "Node Type": "COLUMN",
            "Result Type": "UInt8",
            "Result Name": "1",
            "Column": "Const(UInt8)",
            "Arguments": [],
            "Removed Arguments": [],
            "Result": 1
          }
        ],
        "Outputs": [
          {
            "Name": "1",
            "Type": "UInt8"
          }
        ],
        "Positions": [1]
      },
      "Plans": [
        {
          "Node Type": "ReadFromStorage",
          "Node Id": "ReadFromStorage_0"
        }
      ]
    }
  }
]
```

باستخدام `compact = 0` و`actions = 1`، يمكن رؤية خطوات `Expression` مع معلومات تفصيلية حول التعبيرات:

```sql
EXPLAIN actions = 1, compact = 0 SELECT sum(number) FROM numbers(10) GROUP BY number % 4;
```

```text
Output: sum(number)

Expression ((Project names + Projection))
│  Actions: INPUT : 0 -> sum(__table1.number) UInt64 : 0
│           INPUT :: 1 -> modulo(__table1.number, 4_UInt8) UInt8 : 1
│           ALIAS sum(__table1.number) :: 0 -> sum(number) UInt64 : 2
│  Positions: 2
└──Aggregating
   │  Keys: number MOD 4
   │  Aggregates: sum(number)
   │  Skip merging: 0
   └──Expression ((Before GROUP BY + Change column names to column identifiers))
      │  Actions: INPUT : 0 -> number UInt64 : 0
      │           COLUMN Const(UInt8) -> 4_UInt8 UInt8 : 1
      │           ALIAS number :: 0 -> __table1.number UInt64 : 2
      │           FUNCTION modulo(__table1.number : 2, 4_UInt8 :: 1) -> modulo(__table1.number, 4_UInt8) UInt8 : 0
      │  Positions: 0 2
      └──ReadFromSystemNumbers
            Output: number
```

عند تعيين `distributed` = 1، يتضمن الناتج ليس فقط خطة الاستعلام المحلية، بل أيضاً خطط الاستعلام التي ستُنفَّذ على العقد البعيدة. يُعدّ هذا مفيداً لتحليل الاستعلامات الموزعة وتصحيح أخطائها.

:::note
يُعرض `distributed` فقط في الشكل القديم (غير `pretty`)، لأن مخرجات `pretty` لا تدمج خطط الأجزاء البعيدة في شجرة الخطة. لهذا السبب، يؤدي تفعيل `distributed` تلقائيًا إلى تعطيل الإعدادات الافتراضية لـ `pretty` (وهي `actions` و`compact` و`pretty`)، بصرف النظر عن `explain_query_plan_default`. لا يزال بإمكانك ضبط `actions=1` يدويًا. كما أن خيار `distributed` غير مدعوم مع `json`.
:::

مثال مع جدول موزّع:

```sql
EXPLAIN distributed=1 SELECT * FROM remote('127.0.0.{1,2}', numbers(2)) WHERE number = 1;
```

```sql
Union
  Expression ((Project names + (Projection + (Change column names to column identifiers + (Project names + Projection)))))
    Filter ((WHERE + Change column names to column identifiers))
      ReadFromSystemNumbers
  Expression ((Project names + (Projection + Change column names to column identifiers)))
    ReadFromRemote (Read from remote replica)
      Expression ((Project names + Projection))
        Filter ((WHERE + Change column names to column identifiers))
          ReadFromSystemNumbers
```

مثال مع النسخ المتماثلة المتوازية:

```sql
SET enable_parallel_replicas = 2, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'default';

EXPLAIN distributed=1 SELECT sum(number) FROM test_table GROUP BY number % 4;
```

```sql
Expression ((Project names + Projection))
  MergingAggregated
    Union
      Aggregating
        Expression ((Before GROUP BY + Change column names to column identifiers))
          ReadFromMergeTree (default.test_table)
      ReadFromRemoteParallelReplicas
        BlocksMarshalling
          Aggregating
            Expression ((Before GROUP BY + Change column names to column identifiers))
              ReadFromMergeTree (default.test_table)
```

في كلا المثالَين، تُظهر خطةُ الاستعلام تدفق التنفيذ الكامل بما في ذلك الخطوات المحلية والخطوات عن بُعد.

عند تعيين `pretty` = 1، تُعرض شجرة الخطة باستخدام أحرف رسم الخطوط بدلاً من المسافات البادئة، وتظهر معلومات إضافية للخطوات الرئيسية:

* تُطبع **أعمدة مخرجات الاستعلام** في أعلى الخطة.
* تُعرض **التعبيرات** في عوامل التصفية، ومفاتيح التجميع، وأوصاف الترتيب، ودوال النوافذ بصياغة شبيهة بـ SQL ومقروءة للبشر (مثل `a + 1 > 5` بدلًا من `greater(plus(a, 1), 5)`). وتُزال بادئات معرّفات الأعمدة الداخلية (مثل `__table1.`) لتحسين الوضوح.
* تعرض **خطوات المصدر** (مثل `ReadFromMergeTree`) أعمدة مخرجاتها.
* تعرض **خطوات التصفية** شرط التصفية بصياغة SQL. وعند وجود عوامل تصفية للربط وقت التشغيل، تُعرض بشكل منفصل.
* تعرض **خطوات التجميع** المفاتيح والدوال التجميعية مع وسيطاتها (مثل `sum(c)` و`count()`).
* تعرض **مجموعات IN** الناتجة من القيم الحرفية من نوع tuple قيمها (مع اقتطاعها في المجموعات الكبيرة)، وتُسمّى المجموعات المستندة إلى استعلامات فرعية `subquery1` و`subquery2` وما إلى ذلك، وتعرض المجموعات القادمة من جداول المحرّك `Set` اسم الجدول.
* تعرض **خطوات Join** علاقة الربط باستخدام ترميز رياضي، والعدد التقديري لصفوف النتيجة،
  وأعمدة المخرجات القادمة من الجانب الأيسر مقابل الجانب الأيمن. وتُستخدم الرموز التالية
  لتمثيل أنواع join المختلفة:

| الرمز                  | نوع الربط     |
| ---------------------- | ------------- |
| `⋈`                    | ربط داخلي     |
| `⟕`                    | ربط أيسر      |
| `⟖`                    | ربط أيمن      |
| `⟗`                    | ربط كامل      |
| `⋉`                    | ربط شبه أيسر  |
| `⋊`                    | ربط شبه أيمن  |
| `⋉` with strikethrough | ربط مضاد أيسر |
| `⋊` with strikethrough | ربط مضاد أيمن |
| `×`                    | ربط تبادلي    |

على سبيل المثال، `t1 ⟕ t2` يعني ربطًا أيسر بين الجدولين `t1` و`t2`.
ويشير الرقم بين القوسين بعد اسم الجدول (مثل `t1[100]`) إلى العدد التقديري للصفوف
عندما تكون إحصاءات الجدول متاحة.

يعمل الخيار `pretty` جيدًا مع `compact = 1`، إذ يُخفي خطوات `Expression` ومعلومات الإجراءات التفصيلية، مما يجعل الخطة أسهل قراءةً.

مثال تفصيلي على عمليات الربط:

```sql
CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO t2 SELECT number, toString(number) FROM numbers(100);

EXPLAIN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id FORMAT Raw;
```

```text
Output: id, value, id, value

Join (JOIN FillRightFirst)
│  t1[100] ⋈ t2[100]
│  Type: inner | Strictness: all | Algorithm: SpillingHashJoin(HashJoin)
│  Result rows: 100
│  Join conditions: id = id
│  Output:
│    Left:  id, value
│    Right: id, value
├──ReadFromMergeTree (default.t1)
│     Read type: Default
│     Parts: 1 | Granules: 1
│     Output: id, value
│     Runtime filters: RF1(id, id from default.t2)
└──BuildRuntimeFilter (Build runtime join filter on id)
   │  Filter id: RF1
   │  Source table: default.t2
   └──ReadFromMergeTree (default.t2)
         Read type: Default
         Parts: 1 | Granules: 1
         Output: id, value
```

<div id="explain-pipeline">
  ### EXPLAIN PIPELINE
</div>

الإعدادات:

* `header` — يطبع الترويسة لكل منفذ إخراج. القيمة الافتراضية: 0.
* `graph` — يطبع مخططًا موصوفًا بلغة وصف المخططات [DOT](https://en.wikipedia.org/wiki/DOT_\(graph_description_language\)). القيمة الافتراضية: 0.
* `compact` — يطبع المخطط في وضع مضغوط إذا كان إعداد `graph` مفعّلًا. القيمة الافتراضية: 1.
* `compact_repeated_processor_chains` — يضغط سلاسل المعالجات المتكررة المتجاورة في المخرجات النصية، وذلك بعرض نسخة واحدة من السلسلة مع عدد مرات التكرار. ويمكن أن يجعل ذلك مسارات المعالجة المتوازية أسهل قراءةً عندما تتكرر السلسلة نفسها مرات كثيرة، على سبيل المثال في عمليات الربط. ولا يؤثر ذلك في مخرجات المخطط. القيمة الافتراضية: 0.

```text
Resize 16 → 1
  FillingRightJoinSide          │
    SimpleSquashingTransform    │ × 16
      Resize 1 → 16
```

عندما تكون القيمة `compact=0` و`graph=1`، ستتضمن أسماء المعالجات لاحقةً إضافية تحتوي على معرّف فريد للمعالج.

مثال:

```sql
EXPLAIN PIPELINE SELECT sum(number) FROM numbers_mt(100000) GROUP BY number % 4;
```

```sql
(Union)
(Expression)
ExpressionTransform
  (Expression)
  ExpressionTransform
    (Aggregating)
    Resize 2 → 1
      AggregatingTransform × 2
        (Expression)
        ExpressionTransform × 2
          (SettingQuotaAndLimits)
            (ReadFromStorage)
            NumbersRange × 2 0 → 1
```

<div id="explain-estimate">
  ### EXPLAIN ESTIMATE
</div>

يعرض العدد التقديري للصفوف والعلامات والأجزاء التي ستُقرأ من الجداول أثناء معالجة الاستعلام. يعمل مع الجداول ضمن عائلة [MergeTree](/ar/engines/table-engines/mergetree-family/mergetree).

**مثال**

إنشاء جدول:

```sql title="Query"
CREATE TABLE ttt (i Int64) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity = 16, write_final_mark = 0;
INSERT INTO ttt SELECT number FROM numbers(128);
OPTIMIZE TABLE ttt;
```

```sql title="Query"
EXPLAIN ESTIMATE SELECT * FROM ttt;
```

```text title="Response"
┌─database─┬─table─┬─parts─┬─rows─┬─marks─┐
│ default  │ ttt   │     1 │  128 │     8 │
└──────────┴───────┴───────┴──────┴───────┘
```

<div id="explain-whatif">
  ### EXPLAIN WHATIF
</div>

يقدّر الفائدة التي يمكن أن يحققها فهرس تخطٍ افتراضي لاستعلام `SELECT`، *من دون* تخزين الفهرس فعليًا على القرص. حدِّد مرشحًا واحدًا أو أكثر باستخدام [`CREATE HYPOTHETICAL INDEX`](/ar/sql-reference/statements/hypothetical-index#create-hypothetical-index)، ثم شغّل `EXPLAIN WHATIF SELECT ...` لمعرفة ما يلي لكل مرشح: مدى انطباقه، والعدد التقديري للعلامات المقروءة، والبايتات التقديرية، ونسبة التخطي.

**الصياغة**

```sql
EXPLAIN WHATIF [empirical = 0] SELECT ...
```

**الإعدادات**

* `empirical` — تؤدي القيمة `1` (الافتراضية) إلى تشغيل الفهرس على الحبيبات المُقلَّمة استنادًا إلى خط الأساس داخل الذاكرة لقياس نسبة التخطي (وهي حدّ أعلى). أما القيمة `0` فتتجاوز هذا المسار. وفي كلتا الحالتين، إذا لم يُنتج `empirical` نتيجة (إما لأنه معطّل، أو لأن الفهرس لا يمكن تقييمه في الذاكرة)، فإن المُقدِّر يلجأ إلى [إحصاءات العمود](/ar/engines/table-engines/mergetree-family/mergetree#column-statistics)، ثم أخيرًا إلى ملخّص يقتصر على قابلية التطبيق فقط إذا لم يتوفر أيٌّ منهما.

**المخرجات**

```text
Baseline (after PK + partition + existing indexes):
  table:       db.t
  parts:       1
  marks:       100
  est_bytes:   1.50 MiB             (only when the query reads rows)

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    15.00 KiB           (only when baseline bytes are known)
  skip_ratio:   99.0%

Estimation:
  source:           empirical | statistical | applicability_only
  empirical_status: ok | unsupported | disabled
  sampled_parts:    50 / 100        (only when source = empirical)
  sampled_marks:    50 / 100        (only when source = empirical)
  elapsed_us:       631             (only when source = empirical)
```

* `source` — كيفية احتساب التقدير.
  * `empirical`: بُني الفهرس في الذاكرة على الحبيبات التي بقيت بعد تقليم خط الأساس، ثم عُدَّت الحبيبات التي كان سيتخطاها الفهرس. ويمثل هذا حدًا أعلى — راجع القيود في [`CREATE HYPOTHETICAL INDEX`](/ar/sql-reference/statements/hypothetical-index#limitations).
  * `statistical`: مشتق من إحصاءات الأعمدة. ويُستخدم عند تعطيل التقدير التجريبي (`empirical = 0`) أو عندما يتعذر على التقدير التجريبي إنتاج نتيجة، مع توفّر إحصاءات الأعمدة للأعمدة ذات الصلة.
  * `applicability_only`: الفهرس قابل للتطبيق على الشرط، لكن لا التقدير التجريبي ولا الإحصائي أنتجا نتيجة (مثلًا `empirical = 0` مع عدم تعريف إحصاءات الأعمدة). ويعرض `skip_ratio: 0.0%` كحدٍّ متحفّظ.
* `sampled_parts` / `sampled_marks` — `<baseline-pruned> / <total in the table>`. يوضّح النسبة التي بقيت من الجدول بعد تقليم PK وpartition والفهارس الموجودة، أي ما يُستخدم كمدخل إلى الفهرس الافتراضي.
* `est_bytes` — تقدير للبايتات المقروءة، مشتق من متوسط حجم الصف في الجدول، لذا فهو تقريبي ويختلف بحسب التخزين والضغط. لا يظهر سطر خط الأساس إلا عندما يقرأ الاستعلام صفوفًا؛ ولا يظهر السطر الخاص بكل مرشح إلا عندما يكون تقدير بايتات خط الأساس معروفًا.

يُكتب الإعداد مباشرةً بين `WHATIF` و`SELECT` — ولا توجد الكلمة المفتاحية `SETTINGS` (وهذا يتوافق مع كيفية قبول صيغ `EXPLAIN` الأخرى لخياراتها).

إذا لم تكن هناك فهارس افتراضية معرّفة للجدول، فإن `EXPLAIN WHATIF` يعرض `status: not_applicable` مع تلميح إلى إنشاء فهرس.

**الصف المجمّع (عدة مرشحين)**

عند تقييم مرشحين أو أكثر تجريبيًا، يضيف `EXPLAIN WHATIF` كتلة إضافية واحدة باسم `(combined: idx_a, idx_b, ...)` بعد الصفوف الخاصة بكل مرشح. وتعرض هذه الكتلة الفائدة المشتركة لوجود *كل* هذه الفهارس معًا: فالقراءة الفعلية لا تُبقي الحبيبة إلا إذا اجتازت *كل* فهارس التخطي، لذا يكون التقدير المجمّع هو تقاطع الحبيبات المتبقية لدى المرشحين. لذلك تكون قيمة `skip_ratio` فيه مساوية على الأقل لأفضل مرشح منفرد — فالفهارس المتكاملة تزيد التقليم عند اجتماعها، بينما لا تغيّرها الفهارس المتداخلة.

لا تُسهم إلا المرشَّحات التي تحمل `source: empirical`، لأن الصف الموحَّد يُبنى من تقاطع مجموعات البقاء الخاصة بكل حبيبة لديها. أمّا المرشَّحات المقدَّرة على أنها `statistical` أو `applicability_only` فلا تتوفر لها بيانات على مستوى الحبيبة، لذا تُستبعد. ونتيجةً لذلك، لا تظهر الكتلة الموحَّدة إلا إذا أنتج مرشَّحان على الأقل تقديرًا تجريبيًا، وإلا تُحذف (على سبيل المثال عند `empirical = 0`). وتكون حقول التقدير فيها مماثلة لحقول كتلة تجريبية خاصة بكل مرشَّح، باستثناء أن `elapsed_us` يساوي `0` — إذ إن التقدير الموحَّد مشتق من عمليات المسح الخاصة بكل مرشَّح، وليس من عملية مسح جديدة. والاسم الاصطناعي `(combined: ...)` هو مجرد تسمية في التقرير، ولا يمكن استخدامه مع `force_data_skipping_indices`.

**مثال تجريبي**

```sql
CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100;

INSERT INTO t SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

EXPLAIN WHATIF SELECT * FROM t WHERE b = 42;
```

```text
Baseline (after PK + partition + existing indexes):
  table:       default.t
  parts:       1
  marks:       100
  est_bytes:   85.52 KiB

With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    875.00 B
  skip_ratio:   99.0%

Estimation:
  source:           empirical
  empirical_status: ok
  sampled_parts:    1 / 1
  sampled_marks:    100 / 100
```

سيؤدي `minmax` الافتراضي إلى تقليص عدد العلامات من 100 إلى علامة واحدة — `skip_ratio: 99.0%`. (تمثل القيمة `est_bytes` تقديرًا مستندًا إلى متوسط حجم الصف، لذا قد يختلف الرقم الدقيق.)

**مثال إحصائي**

تكون [إحصاءات الأعمدة](/ar/engines/table-engines/mergetree-family/mergetree#column-statistics) معطّلة افتراضيًا. ولاستخدام المسار `statistical`، عرّفها أولًا على الأعمدة ذات الصلة ثم انتظر حتى تكتمل عملية mutation الخاصة بـ materialize:

```sql
ALTER TABLE t ADD STATISTICS b TYPE TDigest;
ALTER TABLE t MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;
```

ثم عطّل المسار التجريبي ليعتمد المُقدِّر على إحصاءات الأعمدة:

```sql
EXPLAIN WHATIF empirical = 0 SELECT * FROM t WHERE b < 10;
```

```text
With idx_b (minmax, hypothetical):
  status:       applicable
  marks:        1
  est_bytes:    1.66 KiB
  skip_ratio:   99.9%

Estimation:
  source:           statistical
  empirical_status: disabled
```

يأتي هذا الرقم من انتقائية إحصاءات العمود للشرط `b < 10` (حوالي 10 صفوف من أصل 10000)، ويُعرض باعتباره حدًا أعلى لـ `skip_ratio`. لا توجد `sampled_parts` / `sampled_marks` — إذ لم تُقرأ أي بيانات.

إذا لم يكن أيٌّ من المسارين متاحًا (مثلًا، `empirical = 0` وعدم تعريف أي إحصاءات أعمدة)، فإن المُقدِّر يعرض `source: applicability_only` و`skip_ratio: 0.0%` بشكل متحفظ.

<div id="explain-table-override">
  ### EXPLAIN TABLE OVERRIDE
</div>

يعرض نتيجة `table override` على مخطط جدول جرى الوصول إليه عبر `table function`.
ويُجري أيضًا بعض عمليات التحقق، مع إطلاق استثناء إذا كان `override` سيتسبب في أي نوع من الإخفاق.

**مثال**

افترض أن لديك جدول MySQL بعيدًا على النحو التالي:

```sql title="Query"
CREATE TABLE db.tbl (
    id INT PRIMARY KEY,
    created DATETIME DEFAULT now()
)
```

```sql title="Query"
EXPLAIN TABLE OVERRIDE mysql('127.0.0.1:3306', 'db', 'tbl', 'root', 'clickhouse')
PARTITION BY toYYYYMM(assumeNotNull(created))
```

```text title="Response"
┌─explain─────────────────────────────────────────────────┐
│ PARTITION BY uses columns: `created` Nullable(DateTime) │
└─────────────────────────────────────────────────────────┘
```

:::note
لم تكتمل عملية التحقق، لذا فإن نجاح الاستعلام لا يضمن أن OVERRIDE لن يسبب مشكلات.
:::