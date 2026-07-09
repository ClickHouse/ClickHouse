---
description: 'توثيق نوع بيانات JSON في ClickHouse، الذي يوفّر دعمًا أصليًا
  للتعامل مع بيانات JSON'
keywords: ['json', 'نوع البيانات']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'نوع بيانات JSON'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="اطّلع على دليل أفضل الممارسات لاستخدام JSON للحصول على أمثلة وميزات متقدمة واعتبارات تتعلق باستخدام نوع JSON." icon="book" infoText="اقرأ المزيد" infoUrl="/docs/best-practices/use-json-where-appropriate" title="هل تبحث عن دليل؟" />
</Link>

<br />

يخزّن النوع `JSON` مستندات JavaScript Object Notation ‏(JSON) في عمود واحد.

:::note
في ClickHouse Open-Source، تم تصنيف نوع بيانات JSON على أنه جاهز للإنتاج في الإصدار 25.3. ولا يُنصح باستخدام هذا النوع في بيئات الإنتاج في الإصدارات السابقة.
:::

لتعريف عمود من النوع `JSON`، يمكنك استخدام الصيغة التالية:

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```

حيث تُعرَّف المعلَمات في البنية أعلاه كما يلي:

| المعلَمة                    | الوصف                                                                                                                                                                                                                                                                                                                                                                                                                                           | القيمة الافتراضية |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `max_dynamic_paths`         | معلَمة اختيارية تحدد عدد المسارات التي يمكن تخزينها بشكل منفصل كأعمدة فرعية ضمن block واحد من البيانات المخزنة بشكل منفصل (على سبيل المثال، ضمن data part واحد لجدول MergeTree). <br /><br />إذا تم تجاوز هذا الحد، فستُخزَّن جميع المسارات الأخرى معًا في بنية واحدة تُسمى [shared data](#shared-data-structure).<br /><br />توجد أيضًا [طرق](#controlling-the-number-of-dynamic-paths) لتغيير حد المسارات الديناميكية دون تغيير هذه المعلَمة. | `1024`            |
| `max_dynamic_types`         | معلَمة اختيارية تتراوح بين `1` و`255`، وتحدد عدد أنواع البيانات المختلفة التي يمكن تخزينها بشكل منفصل داخل عمود مسار واحد من النوع `Dynamic` ضمن block واحد من البيانات المخزنة بشكل منفصل (على سبيل المثال، ضمن data part واحد لجدول MergeTree). <br /><br />إذا تم تجاوز هذا الحد، فستُخزَّن جميع الأنواع الجديدة معًا في بنية واحدة تُسمى `shared variant`.                                                                                  | `32`              |
| `some.path TypeName`        | تلميح نوع اختياري لمسار محدد في JSON. وستُخزَّن هذه المسارات دائمًا كأعمدة فرعية بالنوع المحدد.                                                                                                                                                                                                                                                                                                                                                 |                   |
| `SKIP path.to.skip`         | تلميح اختياري لمسار محدد يجب تخطيه أثناء تحليل JSON. ولن تُخزَّن هذه المسارات مطلقًا في عمود JSON. وإذا كان المسار المحدد عبارة عن JSON object متداخل، فسيتم تخطي الكائن المتداخل بالكامل.                                                                                                                                                                                                                                                 |                   |
| `SKIP REGEXP 'path_regexp'` | تلميح اختياري يستخدم regular expression لتخطي المسارات أثناء تحليل JSON. ولن تُخزَّن مطلقًا في عمود JSON أي مسارات تطابق هذا regular expression.                                                                                                                                                                                                                                                                                           |                   |

<WhenToUseJson />

<div id="creating-json">
  ## إنشاء `JSON`
</div>

في هذا القسم، نستعرض الطرق المختلفة لإنشاء `JSON`.

<div id="using-json-in-a-table-column-definition">
  ### استخدام `JSON` في تعريف عمود في جدول
</div>

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

<div id="using-cast-with-json">
  ### استخدام CAST مع `::JSON`
</div>

يمكن تحويل أنواع مختلفة باستخدام الصيغة الخاصة `::JSON`.

<div id="cast-from-string-to-json">
  #### CAST من `String` إلى `JSON`
</div>

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-tuple-to-json">
  #### التحويل باستخدام CAST من `Tuple` إلى `JSON`
</div>

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-map-to-json">
  #### التحويل من `Map` إلى `JSON` باستخدام `CAST`
</div>

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
تُخزَّن مسارات JSON بصيغة مُسطَّحة. وهذا يعني أنه عند تكوين كائن JSON من مسار مثل `a.b.c`
لا يمكن معرفة ما إذا كان ينبغي إنشاء الكائن بالشكل `{ "a.b.c" : ... }` أو `{ "a": { "b": { "c": ... } } }`.
وسيفترض تنفيذنا دائمًا الاحتمال الثاني.

على سبيل المثال:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

سيُرجِع:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

و **ليس**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## قراءة مسارات JSON كأعمدة فرعية
</div>

يدعم النوع `JSON` قراءة كل مسار كعمود فرعي مستقل.
إذا لم يكن نوع المسار المطلوب محددًا في تعريف النوع `JSON`،
فسيكون العمود الفرعي لهذا المسار دائمًا من النوع [Dynamic](/ar/sql-reference/data-types/dynamic.md).

على سبيل المثال:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

يمكنك أيضًا استخدام الدالة `getSubcolumn` لقراءة الأعمدة الفرعية من نوع JSON:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

إذا لم يُعثر على المسار المطلوب في البيانات، فسيُملأ بقيم `NULL`:

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

دعونا نتحقق من أنواع بيانات الأعمدة الفرعية المُعادة:

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

كما نرى، بالنسبة إلى `a.b`، يكون النوع `UInt32` كما حدّدناه في تعريف نوع JSON،
أما جميع الأعمدة الفرعية الأخرى فنوعها `Dynamic`.

ويمكن أيضًا قراءة الأعمدة الفرعية من النوع `Dynamic` باستخدام صيغة خاصة `json.some.path.:TypeName`:

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

يمكن تحويل الأعمدة الفرعية في `Dynamic` إلى أي نوع بيانات. وفي هذه الحالة، سيتم طرح استثناء إذا تعذّر تحويل النوع الداخلي داخل `Dynamic` إلى النوع المطلوب:

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
لقراءة الأعمدة الفرعية بكفاءة من أجزاء Compact في MergeTree، تأكد من تفعيل إعداد MergeTree ‏[write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts).
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## قراءة الكائنات الفرعية في JSON كأعمدة فرعية
</div>

يتيح النوع `JSON` قراءة الكائنات المتداخلة كأعمدة فرعية من النوع `JSON` باستخدام صيغة خاصة `json.^some.path`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
عندما تُخزَّن المسارات في [البيانات المشتركة](#shared-data-structure) الأساسية (`map`)، قد تكون قراءة الأعمدة الفرعية للكائنات الفرعية غير فعّالة، لأنها تتطلب فحص بنية البيانات المشتركة بالكامل. أما عند استخدام تسلسل البيانات المشتركة `map_with_buckets` أو `advanced`، فتكون قراءة الأعمدة الفرعية من البيانات المشتركة مُحسّنة بدرجة كبيرة.
:::

<div id="reading-json-combined-sub-columns">
  ## قراءة الأعمدة الفرعية المجمّعة في JSON
</div>

يدعم النوع `JSON` قراءة مسار على أنه **عمود فرعي مجمّع** باستخدام صيغة خاصة `json.@some.path`.
ويُرجع العمود الفرعي المجمّع لمسار معيّن ما يلي:

* القيمة الحرفية المخزّنة في ذلك المسار على هيئة `Dynamic`، إذا كان المسار يحتوي على قيمة حرفية.
* كائن JSON فرعي في ذلك المسار على هيئة `Dynamic`، إذا لم تكن للمسار قيمة حرفية ولكن كانت له مسارات فرعية متداخلة.
* `NULL`، إذا لم تكن هناك قيمة حرفية ولا أي مسارات فرعية موجودة لذلك المسار.

ويكون ذلك مفيدًا عندما قد يحتوي المسار على قيمة مفردة أو كائن متداخل عبر صفوف مختلفة، كما أنه أكثر ملاءمة من الاستعلام بشكل منفصل عن العمود الفرعي الحرفي (`json.a`) والعمود الفرعي للكائن الفرعي (`json.^a`).

يقارن المثال التالي بين الأنواع الثلاثة كلها من الأعمدة الفرعية للمسار `a`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

* Row 1: تحتوي `a` على قيمة حرفية `42`. يعيد `json.a` هذه القيمة على شكل `Dynamic(Int64)`، ويعيد `json.^a` كائنًا فرعيًا فارغًا `{}` (لا توجد مفاتيح متداخلة تحت `a`)، ويعيد `json.@a` القيمة الحرفية `42`.
* Row 2: تحتوي `a` على كائن متداخل. يعيد `json.a` القيمة `NULL` (لا توجد قيمة حرفية في هذا المسار)، ويعيد `json.^a` الكائن الفرعي بصيغة `JSON`، كما يعيد `json.@a` الكائن الفرعي أيضًا على شكل `Dynamic(JSON)`.
* Row 3: `a` غير موجودة أساسًا. يعيد كلٌّ من `json.a` و`json.@a` القيمة `NULL`، بينما يعيد `json.^a` الكائن الفارغ `{}`.

:::note
عندما تُخزَّن المسارات في [البيانات المشتركة](#shared-data-structure) الأساسية (`map`)، قد تكون قراءة الأعمدة الفرعية المجمّعة غير فعّالة لأنها تتطلب فحص بنية البيانات المشتركة بالكامل. ومع `map_with_buckets` أو تنسيق تسلسل البيانات المشتركة `advanced`، تصبح قراءة الأعمدة الفرعية من البيانات المشتركة محسّنة بدرجة كبيرة.
:::

<div id="type-inference-for-paths">
  ## استنتاج النوع للمسارات
</div>

أثناء تحليل `JSON`، يحاول ClickHouse تحديد نوع البيانات الأنسب لكل مسار في JSON.
ويعمل ذلك على نحو مماثل لـ [الاستدلال التلقائي على المخطط من بيانات الإدخال](/ar/interfaces/schema-inference.md)،
ويُتحكَّم فيه عبر الإعدادات نفسها:

* [input&#95;format&#95;try&#95;infer&#95;dates](/ar/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/ar/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/ar/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/ar/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/ar/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/ar/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/ar/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/ar/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/ar/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/ar/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

لنلقِ نظرة على بعض الأمثلة:

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

<div id="handling-arrays-of-json-objects">
  ## التعامل مع مصفوفات من كائنات JSON
</div>

تُحلَّل مسارات JSON التي تحتوي على مصفوفة من الكائنات على أنها من النوع `Array(JSON)`، وتُدرَج في عمود `Dynamic` الخاص بذلك المسار.
ولقراءة مصفوفة من الكائنات، يمكنك استخراجها من عمود `Dynamic` على هيئة عمود فرعي:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

كما قد تكون لاحظت، جرى تقليل المعاملَين `max_dynamic_types`/`max_dynamic_paths` لنوع `JSON` المتداخل مقارنةً بالقيم الافتراضية.
وهذا ضروري لتجنّب ازدياد عدد الأعمدة الفرعية بصورة خارجة عن السيطرة في المصفوفات المتداخلة لكائنات JSON.

لنحاول قراءة الأعمدة الفرعية من عمود `JSON` متداخل:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

يمكننا تجنب كتابة أسماء الأعمدة الفرعية من النوع `Array(JSON)` باستخدام صيغة خاصة:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

يشير عدد الأقواس المربعة `[]` بعد المسار إلى مستوى المصفوفة. على سبيل المثال، سيتحوّل `json.path[][]` إلى `json.path.:Array(Array(JSON))`

لنتحقق من المسارات والأنواع داخل `Array(JSON)`:

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

لنقرأ الأعمدة الفرعية من عمود من النوع `Array(JSON)`:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

يمكننا أيضًا قراءة الأعمدة الفرعية التابعة للكائنات الفرعية من عمود `JSON` متداخل:

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

<div id="handling-json-keys-with-nulls">
  ## التعامل مع مفاتيح JSON ذات القيمة NULL
</div>

في تنفيذ JSON لدينا، يُعتبر `null` وغياب القيمة أمرين متكافئين:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

وهذا يعني أنه يستحيل تحديد ما إذا كانت بيانات JSON الأصلية تتضمن مسارًا ما بقيمة NULL، أم أنها لا تتضمنه أصلًا.

<div id="handling-json-keys-with-dots">
  ## التعامل مع مفاتيح JSON التي تتضمن نقاطًا
</div>

يخزّن عمود JSON داخليًا جميع المسارات والقيم بشكل مُسطَّح. وهذا يعني أنه افتراضيًا يُتعامل مع هذين الكائنين على أنهما متماثلان:

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

سيُخزَّن كلاهما داخليًا على هيئة زوج يتكوّن من المسار `a.b` والقيمة `42`. وعند تنسيق JSON، نُنشئ دائمًا كائنات متداخلة استنادًا إلى أجزاء المسار المفصولة بنقطة:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

كما ترى، جرى الآن تنسيق JSON الأصلي `{"a.b" : 42}` ليصبح `{"a" : {"b" : 42}}`.

ويؤدي هذا القيد أيضًا إلى فشل تحليل كائنات JSON صالحة مثل هذا:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

إذا كنت تريد الاحتفاظ بالمفاتيح التي تحتوي على نقاط وتجنّب تنسيقها ككائنات متداخلة، فيمكنك تفعيل
الإعداد [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/ar/operations/settings/formats#json_type_escape_dots_in_keys) (متاح بدءًا من الإصدار `25.8`). في هذه الحالة، أثناء التحليل ستُحوَّل جميع النقاط في مفاتيح JSON إلى
`%2E` ثم تُفك مرة أخرى أثناء التنسيق.

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

لقراءة مفتاح ذي نقطة مُفلَتة كعمود فرعي، يجب استخدام النقطة المُفلَتة في اسم العمود الفرعي:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

ملاحظة: بسبب قيود محلّل المعرّفات والمحلِّل، يُعدّ العمود الفرعي `` json.`a.b` `` مكافئًا للعمود الفرعي `json.a.b` ولن يقرأ المسار الذي يحتوي على نقطة مُفلتة:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

أيضًا، إذا أردت تحديد تلميح لمسار JSON يحتوي على مفاتيح بها نقاط (أو استخدامه في أقسام `SKIP`/`SKIP REGEX`)، فعليك استخدام النقاط المُفلَتة في التلميح:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

<div id="reading-json-type-from-data">
  ## قراءة نوع JSON من البيانات
</div>

تدعم جميع التنسيقات النصية
([`JSONEachRow`](/ar/interfaces/formats/JSONEachRow),
[`TSV`](/ar/interfaces/formats/TabSeparated),
[`CSV`](/ar/interfaces/formats/CSV),
[`CustomSeparated`](/ar/interfaces/formats/CustomSeparated),
[`Values`](/ar/interfaces/formats/Values)، إلخ) قراءة النوع `JSON`.

أمثلة:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

بالنسبة إلى التنسيقات النصية مثل `CSV`/`TSV`/etc، تُحلَّل قيمة `JSON` من سلسلة نصية تحتوي على كائن JSON:

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

<div id="reaching-the-limit-of-dynamic-paths-inside-json">
  ## بلوغ حدّ المسارات الديناميكية داخل JSON
</div>

لا يمكن لنوع البيانات `JSON` تخزين سوى عدد محدود من المسارات داخليًا على هيئة أعمدة فرعية منفصلة.
ويكون هذا الحد افتراضيًا `1024`، لكن يمكنك تغييره في تعريف النوع باستخدام المعامل `max_dynamic_paths`.

عند بلوغ هذا الحد، ستُخزَّن جميع المسارات الجديدة المُدرجة في عمود `JSON` ضمن بنية بيانات مشتركة واحدة.
ولا يزال من الممكن قراءة هذه المسارات كأعمدة فرعية،
لكن ذلك قد يكون أقل كفاءة ([راجع القسم الخاص بالبيانات المشتركة](#shared-data-structure)).
وهذا الحد ضروري لتجنّب وجود عدد هائل من الأعمدة الفرعية المختلفة قد يجعل الجدول غير قابل للاستخدام.

لنرَ ما يحدث عند بلوغ هذا الحد في عدة سيناريوهات مختلفة.

<div id="reaching-the-limit-during-data-parsing">
  ### الوصول إلى الحد أثناء تحليل البيانات
</div>

أثناء تحليل كائنات `JSON` من البيانات، وعند الوصول إلى الحد في كتلة البيانات الحالية،
ستُخزَّن كل المسارات الجديدة في بنية بيانات مشتركة. ويمكننا استخدام دالّتَي الاستبطان التاليتين `JSONDynamicPaths` و`JSONSharedDataPaths`:

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

كما نرى، بعد إدراج المسارين `e` و`f.g`، جرى بلوغ الحد،
وأُدرجا في بنية بيانات مشتركة.

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### أثناء عمليات دمج أجزاء البيانات في محركات جداول MergeTree
</div>

أثناء دمج عدة أجزاء بيانات في جدول `MergeTree`، قد يصل العمود `JSON` في جزء البيانات الناتج إلى الحد الأقصى للمسارات الديناميكية،
ولا يعود قادرًا على تخزين جميع المسارات من الأجزاء المصدرية كأعمدة فرعية.
في هذه الحالة، يحدّد ClickHouse المسارات التي ستبقى كأعمدة فرعية بعد الدمج، والمسارات التي ستُخزَّن في بنية البيانات المشتركة.
في معظم الحالات، يحاول ClickHouse الاحتفاظ بالمسارات التي تحتوي على
أكبر عدد من القيم غير NULL، ونقل المسارات الأقل شيوعًا إلى بنية البيانات المشتركة. ومع ذلك، يعتمد هذا على آلية التنفيذ.

لنرَ مثالًا على مثل هذا الدمج.
أولًا، لننشئ جدولًا يحتوي على عمود `JSON`، ونضبط الحد الأقصى للمسارات الديناميكية على `3`، ثم نُدرج قيمًا تحتوي على `5` مسارات مختلفة:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

سينشئ كل insert جزء بيانات منفصلًا، بحيث يحتوي العمود `JSON` على مسار واحد:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

الآن، لندمج جميع الأجزاء في جزء واحد ونرَ ما الذي سيحدث:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

كما نرى، احتفظ ClickHouse بالمسارات الأكثر شيوعًا `a` و`b` و`c`، ونقل المسارين `d` و`e` إلى بنية بيانات مشتركة.

<div id="shared-data-structure">
  ## بنية البيانات المشتركة
</div>

كما ذُكر في القسم السابق، عند بلوغ حد `max_dynamic_paths`، تُخزَّن جميع المسارات الجديدة في بنية بيانات مشتركة واحدة.
في هذا القسم، سنتناول تفاصيل بنية البيانات المشتركة وكيفية قراءة الأعمدة الفرعية للمسارات منها.

راجع قسم [&quot;دوال الاستبطان&quot;](/ar/sql-reference/data-types/newjson#introspection-functions) للاطّلاع على تفاصيل الدوال المستخدمة لفحص محتويات عمود JSON.

<div id="shared-data-structure-in-memory">
  ### بنية البيانات المشتركة في الذاكرة
</div>

في الذاكرة، لا تعدو بنية البيانات المشتركة كونها عمودًا فرعيًا من النوع `Map(String, String)` يخزّن ربطًا بين مسار JSON مُسطّح وقيمة مُرمَّزة ثنائيًا.
ولاستخراج عمود فرعي لمسارٍ منه، نمرّ ببساطة على جميع الصفوف في عمود `Map` هذا ونحاول العثور على المسار المطلوب وقيمه.

<div id="shared-data-structure-in-merge-tree-parts">
  ### بنية البيانات المشتركة في أجزاء MergeTree
</div>

في جداول [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)، نُخزّن البيانات في أجزاء بيانات تحفظ كل شيء على القرص (محليًا أو بعيدًا). ويمكن أن تُخزَّن البيانات على القرص بطريقة تختلف عن تخزينها في الذاكرة.
يوجد حاليًا 3 تنسيقات serialization مختلفة لبنية البيانات المشتركة في أجزاء البيانات الخاصة بـ MergeTree: `map` و`map_with_buckets`
و`advanced`.

يتحكم إصدار serialization في إعدادات MergeTree
[object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
و[object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
(الجزء من المستوى الصفري هو الجزء الذي يُنشأ أثناء إدراج البيانات في الجدول، أما أثناء عمليات الدمج فتكون الأجزاء بمستويات أعلى).

ملاحظة: لا يدعم تغيير serialization لبنية البيانات المشتركة إلا
في [إصدار object serialization](../../operations/settings/merge-tree-settings.md#object_serialization_version) `v3`

<div id="shared-data-map">
  #### Map
</div>

في إصدار التسلسل `map`، تُسلسَل shared data كعمود واحد من النوع `Map(String, String)` تمامًا كما تُخزَّن في
الذاكرة. لقراءة عمود فرعي لمسار من هذا النوع من التسلسل، يقرأ ClickHouse عمود `Map` بالكامل ثم
يستخرج المسار المطلوب في الذاكرة.

يكون هذا التسلسل فعّالًا عند كتابة البيانات وقراءة عمود `JSON` بالكامل، لكنه غير فعّال لقراءة أعمدة فرعية للمسارات.

<div id="shared-data-map-with-buckets">
  #### Map مع الحاويات
</div>

في إصدار التسلسل ‏`map_with_buckets`، تُسلسَل البيانات المشتركة في `N` أعمدة (&quot;حاويات&quot;) من النوع `Map(String, String)`.
تحتوي كل حاوية من هذه الحاويات على مجموعة فرعية فقط من المسارات. ولقراءة عمود فرعي لمسار من هذا النوع من التسلسل، يقرأ ClickHouse
عمود `Map` بالكامل من حاوية واحدة ثم يستخرج المسار المطلوب في الذاكرة.

تكون عملية التسلسل هذه أقل كفاءة عند كتابة البيانات وقراءة عمود `JSON` بالكامل، لكنها أكثر كفاءة عند قراءة الأعمدة الفرعية للمسارات
لأنها تقرأ البيانات من الحاويات المطلوبة فقط.

يُتحكَّم في عدد الحاويات `N` بواسطة إعدادات MergeTree [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (8 افتراضيًا)
و [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (32 افتراضيًا).
الحد الأقصى المسموح به لكلا الإعدادين هو 256.

<div id="shared-data-advanced">
  #### متقدم
</div>

في إصدار التسلسل `advanced`، تُسلسَل البيانات المشتركة ضمن بنية بيانات خاصة تُحسِّن إلى أقصى حد أداء
قراءة الأعمدة الفرعية للمسارات، وذلك عبر تخزين معلومات إضافية تتيح قراءة بيانات المسارات المطلوبة فقط.
كما يدعم هذا التسلسل أيضًا الحاويات، بحيث تحتوي كل حاوية على مجموعة فرعية فقط من المسارات.

يُعد هذا التسلسل غير فعّال نسبيًا عند كتابة البيانات (لذلك لا يُنصح باستخدامه مع الأجزاء ذات المستوى الصفري)، كما أن قراءة العمود `JSON` بالكامل تكون أقل كفاءة قليلًا مقارنةً بتسلسل `map`، لكنه فعّال جدًا عند قراءة الأعمدة الفرعية للمسارات.

ملاحظة: بسبب تخزين بعض المعلومات الإضافية داخل بنية البيانات، يكون حجم التخزين على القرص أكبر مع هذا التسلسل مقارنةً
بتسلسلي `map` و`map_with_buckets`.

للاطلاع على نظرة عامة أكثر تفصيلًا على تسلسلات البيانات المشتركة الجديدة وتفاصيل التنفيذ، اقرأ [منشور المدونة](https://clickhouse.com/blog/json-data-type-gets-even-better).

<div id="controlling-the-number-of-dynamic-paths">
  ## التحكم في عدد المسارات الديناميكية داخل JSON في أجزاء MergeTree
</div>

الطريقة الرئيسية لفرض حدّ على المسارات الديناميكية في JSON هي استخدام المعلمة `max_dynamic_paths` ضمن تعريف نوع JSON.
لكن تغيير `max_dynamic_paths` للأعمدة الحالية يتطلب تشغيل `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`، ما يبدأ عملية mutation في الخلفية تعيد كتابة جميع الأجزاء الحالية.
وقد تكون هذه العملية ثقيلة جدًا وتؤثر في أداء الخادم حتى اكتمالها. ولتجنب ذلك، يمكنك استخدام هذه الإعدادات الثلاثة التي تساعدك على تغيير الحد الأقصى للمسارات الديناميكية في جداول MergeTree لأجزاء البيانات الجديدة:

* `merge_max_dynamic_subcolumns_in_wide_part` - إعداد MergeTree يحدّ من عدد الأعمدة الفرعية الديناميكية لكل عمود JSON أثناء الدمج في جزء بيانات Wide.
* `merge_max_dynamic_subcolumns_in_compact_part` - إعداد MergeTree يحدّ من عدد الأعمدة الفرعية الديناميكية لكل عمود JSON أثناء الدمج في جزء بيانات Compact.
* `max_dynamic_subcolumns_in_json_type_parsing` - إعداد جلسة يحدّ من عدد الأعمدة الفرعية الديناميكية لكل عمود JSON أثناء تحليل بيانات JSON إلى عمود JSON.

ملاحظة: لا يمكن أن يتجاوز حدّ المسارات الديناميكية القيمة المحددة في المعلمة `max_dynamic_paths`، حتى إذا كانت قيم الإعدادات المذكورة أعلى.

<div id="introspection-functions">
  ## دوال الاستبطان
</div>

توجد عدة دوال تساعد في فحص محتوى عمود JSON:

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**أمثلة**

لنستعرض محتوى مجموعة بيانات [GH Archive](https://www.gharchive.org/) للتاريخ `2020-01-01`:

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

<div id="alter-modify-column-to-json-type">
  ## ALTER MODIFY COLUMN إلى نوع JSON
</div>

يمكن تعديل جدول موجود وتغيير نوع العمود إلى نوع `JSON` الجديد. حاليًا، لا يُدعَم `ALTER` إلا عند التحويل من النوع `String`.

**مثال**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

<div id="lazy-type-hints">
  ## تلميحات الأنواع الكسولة (تجريبي)
</div>

:::note
هذه الميزة تجريبية وتتطلب تمكين الإعداد `allow_experimental_json_lazy_type_hints`.
:::

عندما تضيف تلميحات الأنواع أو تعدّلها في عمود JSON باستخدام `ALTER TABLE ... MODIFY COLUMN`، يعيد ClickHouse عادةً كتابة جميع أجزاء البيانات لتجسيد تلميحات الأنواع الجديدة فعليًا. وقد تكون هذه العملية مكلفة للغاية للجداول التي تحتوي على كميات كبيرة من البيانات التاريخية (مئات التيرابايتات).

تتيح **تلميحات الأنواع الكسولة** إضافة تلميحات الأنواع كعملية تقتصر على البيانات الوصفية فقط، من دون إعادة كتابة البيانات الموجودة:

* **الأجزاء القديمة**: تُطبَّق تلميحات الأنواع وقت تنفيذ الاستعلام عبر التحويل من `Dynamic` إلى النوع المحدَّد في التلميح
* **الأجزاء الجديدة**: تُجسَّد تلميحات الأنواع فعليًا أثناء عمليات `INSERT`
* **عمليات الدمج**: تُجسَّد تلميحات الأنواع فعليًا عند دمج الأجزاء

وهذا يعني أنه يمكنك إضافة تلميحات الأنواع فورًا، وستُحوَّل البيانات تدريجيًا مع حدوث عمليات الدمج العادية في الخلفية.

<div id="enabling-lazy-type-hints">
  ### تمكين تلميحات الأنواع الكسول
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### مثال
</div>

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

<div id="verifying-no-mutation-occurred">
  ### التحقق من عدم حدوث أي Mutation
</div>

يمكنك التحقق من أن `ALTER` اكتمل من دون إنشاء Mutation عبر فحص الجدول `system.mutations`:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

مع تفعيل تلميحات الأنواع الكسولة، لا يُرجع هذا الاستعلام أي صفوف، مما يؤكد أن العملية اقتصرت على البيانات الوصفية فقط.

<div id="materializing-type-hints">
  ### تجسيد تلميحات النوع
</div>

لتجسيد تلميحات النوع في البيانات الموجودة، يمكنك إما:

1. **انتظار عمليات الدمج في الخلفية**: سيقوم ClickHouse تلقائيًا بتجسيد تلميحات النوع عند دمج الأجزاء
2. **فرض الدمج**: استخدم `OPTIMIZE TABLE test_lazy FINAL` لدمج جميع الأجزاء فورًا
3. **إعادة كتابة الأجزاء**: استخدم `ALTER TABLE test_lazy REWRITE PARTS` لإعادة كتابة الأجزاء باستخدام البيانات الوصفية الجديدة

<div id="lazy-type-hints-limitations">
  ### القيود
</div>

* هذه الميزة تجريبية وقد تتغير في الإصدارات المستقبلية
* قد يترتب على تحويل الأنواع وقت الاستعلام حملٌ كبير على الأداء مقارنةً بالأنواع المُجسَّدة مسبقًا، خاصةً مع كائنات JSON الكبيرة
* لا تنطبق هذه الميزة إلا عند تعديل `typed_paths` (تلميحات الأنواع)؛ أما معلمات JSON الأخرى مثل `max_dynamic_paths` أو `SKIP` أو `SKIP REGEXP` فما زالت تتطلب عمليات mutation

<div id="comparison-between-values-of-the-json-type">
  ## مقارنة بين قيم النوع JSON
</div>

تُقارَن كائنات JSON على نحوٍ مماثل لـ Maps.

على سبيل المثال:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**ملاحظة:** عندما يحتوي مساران على قيم من أنواع بيانات مختلفة، تُقارَن هذه القيم وفق [قاعدة المقارنة](/ar/sql-reference/data-types/variant#comparing-values-of-variant-data) الخاصة بنوع البيانات `Variant`.

<div id="data-skipping-indexes-for-json">
  ## فهارس تخطي البيانات لـ JSON
</div>

يمكن استخدام [فهارس تخطي البيانات](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) مع أعمدة `JSON` بثلاث طرق:

1. **فهارس على أعمدة فرعية محددة** — أنشئ فهرس تخطٍّ قياسيًا على مسار JSON معروف، تمامًا كما تفعل مع عمود عادي. يفهرس هذا *القيم* الموجودة في ذلك المسار.
2. **فهارس قائمة على المسار باستخدام `JSONAllPaths`** — افهرس *مجموعة المسارات* الموجودة في كل حبيبة لتخطي الـ حبيبات التي يستحيل أن تحتوي على المسار المطلوب في الاستعلام.
3. **فهارس قائمة على القيم باستخدام `JSONAllValues`** — افهرس *جميع القيم* عبر كل مسارات JSON باستخدام [فهرس نصي](/ar/engines/table-engines/mergetree-family/textindexes.md) لتسريع البحث النصي الكامل في أي عمود فرعي لـ JSON باستخدام فهرس واحد.

<div id="json-indexes-on-subcolumns">
  ### فهارس على أعمدة فرعية محددة
</div>

يمكنك إنشاء فهرس تخطٍّ على أي عمود فرعي في JSON باستخدام الصياغة نفسها المستخدمة مع الأعمدة العادية.
ويعمل أي [نوع فهرس مدعوم](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) (`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1`، إلخ).

هناك طريقتان للإشارة إلى عمود فرعي في JSON داخل تعبير فهرس:

* **مسار محدد النوع** مُعلن في type hint الخاص بـ JSON — يُستخدم الاسم مباشرة: `json.a`.
* **مسار Dynamic** مع cast صريح — استخدم صياغة cast `::`: `json.b::String`.

يمكنك أيضًا استخدام تعبيرات تجمع بين عدة أعمدة فرعية، على سبيل المثال `json.a || json.b::String`.

<div id="json-indexes-on-subcolumns-example">
  #### مثال
</div>

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

يُضيّق فهرس `minmax` على العمود الفرعي المحدَّد النوع `data.sensor_id` نطاق الفحص ليقتصر على الحبيبات المطابقة:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

يعمل أيضًا فهرس `bloom_filter` على العمود الفرعي المُحوَّل `data.location::String`:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  ### الفهارس المعتمدة على المسارات باستخدام JSONAllPaths
</div>

يمكن أيضًا إنشاء [فهارس تخطي البيانات](/ar/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) على أعمدة `JSON` باستخدام الدالة [`JSONAllPaths`](/ar/sql-reference/functions/json-functions#JSONAllPaths).
ويعمل ذلك بطريقة مماثلة لإنشاء فهارس التخطي على أعمدة [`Map`](/ar/sql-reference/data-types/map) عبر `mapKeys` — إذ يخزّن الفهرس مجموعة مسارات JSON الموجودة في كل حبيبة، ويستخدمها لتخطي الـ حبيبات التي لا يمكن أن تحتوي على المسار المُستعلَم عنه.

<div id="json-indexes-jsonallpaths-supported-types">
  #### أنواع فهارس التخطي المدعومة
</div>

يمكن استخدام `JSONAllPaths` مع أنواع فهارس التخطي التالية:

* [`bloom_filter`](/ar/engines/table-engines/mergetree-family/mergetree#bloom-filter) — يدعم `equals` و`in` و`IS NOT NULL`.
* [`tokenbf_v1`](/ar/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — يدعم `equals` و`IS NOT NULL`.
* [`ngrambf_v1`](/ar/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — يدعم `equals` و`IS NOT NULL`.
* [`text`](/ar/engines/table-engines/mergetree-family/textindexes) (فهرس معكوس) — يدعم `equals` و`in` و`IS NOT NULL`.

<div id="json-indexes-on-subcolumns-example">
  #### مثال
</div>

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

يمكنك استخدام `EXPLAIN indexes = 1` للتأكد من استخدام فهرس تخطٍّ. عندما يوجد مسار في جزء واحد فقط، يتجاوز الفهرس الجزء الآخر:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

عندما لا يكون المسار موجودًا في أي جزء، تُتخطّى جميع الأجزاء والحبيبات:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

يستخدم `IS NOT NULL` أيضًا الفهرس — إذ يتخطّى الحبيبات التي يكون فيها المسار غير موجود (لأن القيمة ستكون `NULL`):

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallpaths-how-it-works">
  #### كيف يعمل
</div>

ينتج التعبير `JSONAllPaths(json_column)` قيمة من النوع `Array(String)` تحتوي على جميع المسارات الموجودة في قيمة JSON.
يخزّن فهرس تخطٍّ سلاسل المسارات هذه في بنية البيانات الخاصة به (bloom filter أو فهرس مقلوب).
عندما يطبّق الاستعلام عامل تصفية على `json.some.path`، يتحقق الفهرس مما إذا كانت السلسلة `"some.path"` موجودة في الفهرس لكل حبيبة، ويتخطى الحبيبات التي لا تكون موجودة فيها.

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### الأمان عند غياب المسارات
</div>

عندما يكون مسار JSON غير موجود في حبيبة، فسيُقيَّم العمود الفرعي إلى:

* `NULL` في النوع `Dynamic` (مثل `json.path`) والأعمدة الفرعية من النوع `Nullable` (مثل `json.path.:Int64`) — إذ إن المقارنات مع `NULL` تُرجع دائمًا false، لذا يكون skipping آمنًا.
* القيمة الافتراضية للنوع في تعبيرات CAST غير `Nullable` (على سبيل المثال، ينتج `json.path::Int64` القيمة `0` عندما يكون المسار غير موجود) — ولا يكون skipping آمنًا إلا إذا كانت القيمة المُقارَن بها مختلفة عن القيمة الافتراضية. ويتولى الفهرس هذا التمييز تلقائيًا.

<div id="json-indexes-jsonallvalues">
  ### البحث في النص الكامل باستخدام JSONAllValues
</div>

يمكن استخدام [فهارس النص](/ar/engines/table-engines/mergetree-family/textindexes.md) لتسريع البحث في النص الكامل في أعمدة JSON من خلال الدالة [`JSONAllValues`](/ar/sql-reference/functions/json-functions#JSONAllValues).
تعيد `JSONAllValues` جميع القيم من عمود JSON على هيئة `Array(String)`، ويمكن فهرستها باستخدام فهرس نصي.
يغطي فهرس واحد على `JSONAllValues(json_column)` جميع مسارات JSON، مما يتيح البحث في النص الكامل في أي عمود فرعي دون الحاجة إلى إنشاء فهارس منفصلة لكل مسار.

راجع [الفهارس المستندة إلى القيم باستخدام JSONAllValues](/ar/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) في توثيق فهارس النص للاطلاع على التفاصيل والأمثلة.

<div id="tips-for-better-usage-of-the-json-type">
  ## نصائح لتحسين استخدام نوع JSON
</div>

قبل إنشاء عمود `JSON` وتحميل البيانات إليه، ضع النصائح التالية في الاعتبار:

* افحص بياناتك وحدد أكبر عدد ممكن من تلميحات المسارات مع أنواعها. فهذا يجعل التخزين والقراءة أكثر كفاءة بكثير.
* فكّر في المسارات التي ستحتاج إليها والمسارات التي لن تحتاج إليها إطلاقًا. حدّد المسارات التي لن تحتاج إليها في قسم `SKIP`، وفي قسم `SKIP REGEXP` عند الحاجة. سيؤدي ذلك إلى تحسين التخزين.
* لا تضبط المعلَمة `max_dynamic_paths` على قيم مرتفعة جدًا، لأن ذلك قد يقلل من كفاءة التخزين والقراءة.
  ورغم أن ذلك يعتمد بدرجة كبيرة على معلمات النظام مثل الذاكرة وCPU وغير ذلك، فإن القاعدة العامة هي ألّا تضبط `max_dynamic_paths` على قيمة أكبر من 10 000 لتخزين نظام الملفات المحلي، و1024 لتخزين نظام الملفات البعيد.

<div id="further-reading">
  ## للمزيد من القراءة
</div>

* [كيف بنينا نوع بيانات JSON جديدًا وقويًا لـ ClickHouse](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [تحدي JSON لمليار مستند: ClickHouse في مواجهة MongoDB وElasticsearch وغيرهما](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)