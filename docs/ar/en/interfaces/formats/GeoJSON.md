---
alias: []
description: 'تنسيق إدخال وإخراج لمستندات GeoJSON FeatureCollection: عند الإدخال، صف واحد لكل عنصر مع أعمدة id وgeometry وproperties؛ وعند الإخراج، عنصر واحد لكل صف.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| الإدخال | الإخراج | الاسم المستعار |
| ------- | ------- | -------------- |
| ✔       | ✔       |                |

<div id="description">
  ## الوصف
</div>

تُنقل بيانات [GeoJSON](https://geojson.org/) على هيئة مستند [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) واحد، ويُطابقها ClickHouse مع ثلاثة أعمدة — `id` و`geometry` و`properties` — مجموعة واحدة لكل `Feature`. تؤدي [قراءة](#reading-data) مستند إلى إنتاج صف واحد لكل عنصر؛ بينما تؤدي [كتابة](#writing-data) إلى إنتاج عنصر واحد لكل صف.

<div id="reading-data">
  ## قراءة البيانات
</div>

ينتج عن قراءة `FeatureCollection` صف واحد لكل عنصر وفق المخطط الثابت التالي:

| العمود       | النوع              | الوصف                                                                                                                                                                                     |
| ------------ | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | العضو `id` الخاص بالعنصر (سلسلة `JSON` أو رقم)، ويُخزَّن كنص؛ وتكون القيمة `NULL` إذا كان `id` غير موجود أو كانت قيمته `null`، بينما يُحتفَظ بمعرّف السلسلة الفارغة الصريح على هيئة `''`. |
| `geometry`   | `Geometry`         | هندسة العنصر، وتُخزَّن كنوع `Geometry` من `Variant`.                                                                                                                                      |
| `properties` | `Nullable(JSON)`   | الكائن `properties` الخاص بالعنصر، ويُخزَّن في عمود `JSON` شبه منظَّم. وتُحفَظ القيمة الصريحة `"properties": null` على هيئة `NULL`.                                                       |

تُخزَّن كل هندسة في النوع `Geometry` في ClickHouse (وهو `Variant`). أنواع هندسة GeoJSON المدعومة هي `Point` و`LineString` و`MultiLineString` و`Polygon` و`MultiPolygon`. أما نوعا هندسة GeoJSON الآخران، `GeometryCollection` و`MultiPoint`، فلا يمكن تمثيلهما بالنوع `Geometry`؛ وتؤدي قراءة أحدهما في العمود `geometry` إلى ظهور استثناء افتراضيًا، ويمكن تغيير هذا السلوك لإدراج `NULL` بدلًا من ذلك — انظر [التعامل مع أنواع الهندسة غير المدعومة](#unsupported-geometry) أدناه. افتراضيًا، لا تكون قيمة العمود `geometry` هي `NULL` إلا عندما تكون هندسة العنصر قيمة `null` صريحة في `JSON`؛ ومع `input_format_geojson_unsupported_geometry_handling = 'null'` تصبح أيضًا `NULL` في حالة نوع هندسة غير مدعوم.

يتم التحقق من بنية المستند: يجب أن تكون قيمة `type` في المستوى الأعلى هي `FeatureCollection`، ويجب أن تكون قيمة `type` لكل عنصر ضمن `features` مساوية لـ `Feature`. افتراضيًا، يجب أن تستوفي الإحداثيات قيود البنية في GeoJSON — إذ يجب أن يحتوي `LineString` (وكل خط في `MultiLineString`) على نقطتين على الأقل، ويجب أن تكون الحلقة في `Polygon` (وكل حلقة في `MultiPolygon`) مغلقة وأن تحتوي على أربع نقاط على الأقل (انظر [التحقق من صحة الهندسة](#geometry-validation)). تُرفَض المستندات غير الصالحة بدلًا من تحميلها بصمت.

ترتيب المفاتيح مرن: يمكن أن تظهر `type` في المستوى الأعلى قبل المصفوفة `features` أو بعدها، وداخل كائن الهندسة يمكن أن تظهر `coordinates` قبل `type` أو بعدها.

يعيد استنتاج المخطط المخطط الثابت أعلاه، لذا يعمل `DESCRIBE` و`SELECT ... FROM format(...)` بدون تعريف جدول.

بالنظر إلى ملف GeoJSON التالي `london.geojson` الذي يحتوي على مزيج من أنواع الهندسة:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [-0.0761, 51.5081]},
            "properties": {"name": "Tower of London", "feature_type": "landmark", "year_built": 1078}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-0.2500, 51.4700], [-0.1800, 51.4900], [-0.1200, 51.5060], [-0.0700, 51.5050], [0.0000, 51.5100]]
            },
            "properties": {"name": "River Thames", "feature_type": "river", "length_km": 346}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-0.1880, 51.5074], [-0.1533, 51.5074], [-0.1533, 51.5153], [-0.1880, 51.5153], [-0.1880, 51.5074]]]
            },
            "properties": {"name": "Hyde Park", "feature_type": "park", "area_km2": 1.42}
        }
    ]
}
```

يمكننا تنفيذ استعلام على الملف ومعاينة أنواع الأشكال الهندسية:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name────────────┬─geo_type───┐
│ 1  │ Tower of London │ Point      │
│ 2  │ River Thames    │ LineString │
│ 3  │ Hyde Park       │ Polygon    │
└────┴─────────────────┴────────────┘
```

يُكتشَف امتداد الملف `.geojson` تلقائيًا، لذا يمكن الاستغناء عن وسيطة التنسيق:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

يمكننا استخدام `variantType` للتحقق من النوع الفعلي لكل كائن Geometry:

```sql title="Query"
SELECT properties.name AS name, geometry, variantType(geometry)
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
geometry:              (-0.0761,51.5081)
variantType(geometry): Point

Row 2:
──────
name:                  River Thames
geometry:              [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
variantType(geometry): LineString

Row 3:
──────
name:                  Hyde Park
geometry:              [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
variantType(geometry): Polygon
```

ويمكننا استخراج البيانات الأساسية على النحو التالي:

```sql title="Query"
SELECT properties.name AS name, variantType(geometry), geometry.Point, geometry.LineString, geometry.Polygon
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
variantType(geometry): Point
geometry.Point:        (-0.0761,51.5081)
geometry.LineString:   []
geometry.Polygon:      []

Row 2:
──────
name:                  River Thames
variantType(geometry): LineString
geometry.Point:        (0,0)
geometry.LineString:   [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
geometry.Polygon:      []

Row 3:
──────
name:                  Hyde Park
variantType(geometry): Polygon
geometry.Point:        (0,0)
geometry.LineString:   []
geometry.Polygon:      [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
```

يُرجِع الوصول إلى عمود فرعي من `Geometry` القيمة إذا كان الصف يحتوي على ذلك النوع، وإلا فيُرجِع القيمة الافتراضية لذلك النوع — `(0,0)` لـ `Point` و`[]` للأنواع المستندة إلى المصفوفات — لذا استخدم `variantType(geometry)` لمعرفة النوع المعيَّن.

يمكننا أيضًا إدخال بيانات GeoJSON إلى جدول:

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   Nullable(JSON),
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO london
SELECT id, geometry, properties
FROM file('london.geojson', GeoJSON);
```

ثم استعلم حسب نوع الكائن:

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM london
ORDER BY id;
```

```response title="Response"
┌─name────────────┬─feature_type─┬─geo_type───┐
│ Tower of London │ landmark     │ Point      │
│ River Thames    │ river        │ LineString │
│ Hyde Park       │ park         │ Polygon    │
└─────────────────┴──────────────┴────────────┘
```

يمكننا أيضًا استنتاج مخطط بيانات GeoJSON من دون تعريف جدول:

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ Nullable(JSON)   │
└────────────┴──────────────────┘
```

<div id="unsupported-geometry">
  ### التعامل مع أنواع هندسة GeoJSON غير المدعومة
</div>

بعض أنواع هندسة GeoJSON الصالحة — مثل `GeometryCollection` و`MultiPoint` — لا يمكن تمثيلها بالنوع `Geometry` في ClickHouse. يمكنك التحكم في ما يحدث عندما يلزم تخزين هندسة من هذا النوع في العمود `geometry` باستخدام الإعداد `input_format_geojson_unsupported_geometry_handling`. القيم الممكنة هي:

* `'throw'` — طرح استثناء (افتراضيًا)
* `'null'` — إدراج قيمة `NULL` في العمود `geometry` ومتابعة التحليل

ينطبق هذا السلوك فقط عند قراءة العمود `geometry`. وعندما لا يكون `geometry` ضمن أعمدة الإخراج المطلوبة (على سبيل المثال `SELECT id FROM ...`)، تظل الهندسة غير المدعومة خاضعة للتحقق من صحة البنية، لكنها لا تفعّل هذا السلوك — فلا يُطرح استثناء ولا تُدرج قيمة `NULL`، لأنه لا يتم إنشاء أي قيمة للهندسة فعليًا.

<div id="reading-limitations">
  ### القيود
</div>

تعكس عملية القراءة فقط ما يتوافق مع المخطط الثابت، لذلك لا تُحفَظ بعض معلومات GeoJSON:

* لا يُنتَج سوى `id` و`geometry` و`properties`؛ أما بقية بنية المستند فلا تُعرَض كأعمدة.
* يُهمَل الإحداثي الثالث للموضع (الارتفاع)، وكذلك أي إحداثيات تليه — فتُصبح المواضع `[longitude, latitude]`.
* يتم تجاهل `bbox` والأعضاء الخارجية (مثل `name` أو `crs` على المستوى الأعلى، أو الأعضاء الإضافيين داخل `Feature`).
* يُخزَّن `id` الرقمي كنص، لذا يُفقَد التمييز بين السلسلة النصية والرقم؛ أما `id` غير الموجود أو ذو القيمة `null` فيصبح `NULL`.
* لا يمكن تمثيل `GeometryCollection` و`MultiPoint` — راجع [التعامل مع أنواع الأشكال الهندسية غير المدعومة](#unsupported-geometry).

<div id="writing-data">
  ## كتابة البيانات
</div>

ينتج عن كتابة مجموعة نتائج `FeatureCollection` واحدة بتنسيق GeoJSON من نوع [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3)، بحيث تقابل كل `Feature` صفًا واحدًا.

تُحوَّل أعمدة النتيجة إلى كل `Feature` على النحو التالي:

| عضو `Feature` | يُنشأ من                       | ملاحظات                                                                                                                                                                                                                                                                                   |
| ------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`        | —                              | تكون قيمته دائمًا `"Feature"`.                                                                                                                                                                                                                                                            |
| `geometry`    | العمود الوحيد ذي النوع الهندسي | يجب وجود عمود واحد فقط ذي نوع هندسي، وإلا يُرفض الاستعلام. وتُكتب قيمة `NULL` في `geometry` على هيئة `null`.                                                                                                                                                                              |
| `id`          | عمود اسمه `id`                 | يُهمَل عندما تكون القيمة `NULL`. يُكتب عمود `String` كسلسلة JSON، ويُكتب العمود الرقمي كرقم JSON.                                                                                                                                                                                         |
| `properties`  | جميع الأعمدة المتبقية          | إذا وُجد عمود واحد باسم `properties` وكان نوعه شبيهًا بالكائن (`JSON` أو `Map` أو `Tuple` مسمّاة)، فيُكتب مباشرةً ككائن `properties` بدلًا من تضمينه داخل مفتاح `properties`. وفي غير ذلك، يتحول كل عمود متبقٍ إلى خاصية بمفتاح يحمل اسمه (ويكون الناتج كائنًا فارغًا إذا لم توجد أعمدة). |

يمكن أن يكون العمود ذو النوع الهندسي هو المتغير `Geometry` أو نوعًا جغرافيًا محددًا؛ ويقابل كلٌّ منها نوعًا هندسيًا في GeoJSON:

| نوع ClickHouse    | GeoJSON `"type"`              |
| ----------------- | ----------------------------- |
| `Point`           | `Point`                       |
| `LineString`      | `LineString`                  |
| `MultiLineString` | `MultiLineString`             |
| `Polygon`         | `Polygon`                     |
| `MultiPolygon`    | `MultiPolygon`                |
| `Ring`            | `Polygon` (حلقة واحدة)        |
| `Geometry`        | نوع المتغير النشط (أو `null`) |

لا يُعد `Ring` نوعًا هندسيًا في GeoJSON — فـ [الحلقة الخطية](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) هي مكوّن من `Polygon` — لذلك تُكتب قيمة `Ring` على أنها `Polygon` أحادية الحلقة.

<div id="writing-examples">
  ### أمثلة
</div>

استكمالًا لجدول `london` [الذي أُنشئ أعلاه](#reading-data)، فإن تصدير أعمدة السمات العادية يحوّل كل عمود باستثناء `id` و`geometry` إلى خاصية:

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

نظرًا لأن العمود الوحيد من النوع object والمسمى `properties` يُكتَب مباشرةً، فإن قراءة ملف GeoJSON ثم إعادة كتابته كما هو تعيد إنتاج المستند (إذ إن الأعمدة `id` و`geometry` و`properties` هي الأعمدة المستنتجة من الملف):

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

يُكتَب عمود `id` العددي كقيمة رقمية في JSON (ويُحذَف تمامًا إذا كان `id` من النوع `Nullable` وكانت قيمته `NULL`):

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

يُمثَّل `Ring` على شكل `Polygon` أحادي الحلقة:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### الكتابة إلى ملف
</div>

استخدم `INTO OUTFILE` لكتابة ملف GeoJSON على جانب العميل:

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

يمكن للخادم نفسه كتابة الملف باستخدام دالة الجدول `file` (يختار الامتداد `.geojson` التنسيق تلقائيًا):

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="reading-limitations">
  ### القيود
</div>

:::note
لا تتضمن الأنواع الجغرافية في ClickHouse أي نظام مرجعي للإحداثيات، لذا يفترض الناتج أن الإحداثيات هي أصلًا إحداثيات WGS84 بخطي الطول/العرض وبالترتيب `[longitude, latitude]`، كما يقتضي [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4). ولا يُجرى أي تحويل للإسقاط أو تبديل للمحاور، لذلك فإن الإحداثيات المسقطة — أو البيانات المخزنة بالصيغة `(latitude, longitude)` — تنتج GeoJSON صحيحًا بنيويًا لكنه غير مطابق للمواصفة.
:::

لا يعكس الناتج إلا ما يخزّنه ClickHouse:

* لا يمكن إعادة إنتاج المعلومات التي تُفقَد عند القراءة — مثل ارتفاع موضعٍ ما، و`bbox`، والأعضاء الخارجية، والتمييز في `id` بين كونه سلسلة نصية أو رقمًا —؛ راجع [قيود القراءة](#reading-limitations).
* تُكتب الإحداثيات من قيم `Float64` باستخدام أقصر تمثيل يمكن تحويله ذهابًا وإيابًا دون تغيير.
* يُخرَج كائن `properties` المأخوذ مباشرةً من عمود `JSON` وفق الترتيب المعياري لمفاتيح نوع `JSON`، وقد يختلف ذلك عن المُدخل.

تُكتب الهندسات تمامًا كما هي مخزنة — مع الحفاظ على ترتيب الإحداثيات واتجاه الالتفاف. افتراضيًا، يُفرَض التحقق من صحة شكل GeoJSON عند الكتابة (راجع [التحقق من صحة الهندسة](#geometry-validation)): فيُرفَض أي هندسة لا تمثّل شكل GeoJSON صالحًا، مثل `LineString` يحتوي على نقطة واحدة أو حلقة `Polygon` غير مغلقة، حتى يمكن قراءة المستند المكتوب مرة أخرى. اضبط `format_geojson_validate_geometry = 0` لإخراج هذه الهندسات كما هي بدلًا من ذلك، مما ينتج GeoJSON صحيحًا بنيويًا لكنه غير مطابق للمواصفة. كذلك لا تُفرَض قاعدة اليد اليمنى (اتجاه الالتفاف) في أي من الحالتين، كما يُحفَظ التمييز بين `null` وكائن `properties` الفارغ.

<div id="geometry-validation">
  ## التحقق من صحة الهندسة
</div>

يتحكم الإعداد `format_geojson_validate_geometry` في ما إذا كان التنسيق يفرض قواعد شكل الهندسة وفقًا لـ [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1)، في كلا الاتجاهين. وهو مفعّل افتراضيًا.

عند تفعيله، تُرفض أي هندسة تخالف قواعد شكل GeoJSON: ‏`LineString` (أو خط داخل `MultiLineString`) يحتوي على أقل من نقطتين؛ أو حلقة في `Polygon` أو `MultiPolygon` تحتوي على أقل من أربع نقاط، أو تكون نقطتها الأولى مختلفة عن الأخيرة (حلقة غير مغلقة)؛ أو `MultiLineString` أو `Polygon` أو `MultiPolygon` فارغ. وتنطبق القواعد نفسها عند قراءة مثل هذا المستند وعند كتابة مثل هذه القيمة في ClickHouse، لذلك يمكن دائمًا قراءة المستند المكتوب مرة أخرى.

عند تعطيله، لا تُفرض قواعد الشكل هذه في أي من الاتجاهين: تُقرأ الأشكال الهندسية المنحلة كما هي وتُكتب كما هي. ويتيح ذلك لقيم الهندسة في ClickHouse التي لا تمثل أشكال GeoJSON هندسية صالحة أن تمر عبر التنسيق ذهابًا وإيابًا، مقابل إنتاج مستندات ليست GeoJSON صالحة.

يقتصر التحقق على البنية فقط: فهو يفحص عدد النقاط وإغلاق الحلقات. ولا يفحص الصحة الهندسية للشكل، لذلك تُقبل الهندسة الصحيحة بنيويًا ولكن المنحلة هندسيًا في أي من الاتجاهين — مثل مضلع مساحته صفر، أو حلقة ذات تقاطع ذاتي، أو مضلع تقع ثقوبه (الحلقات الداخلية) خارج حلقته الخارجية. وبالمثل، لا يُفرض مطلقًا اتجاه قاعدة اليد اليمنى (winding) لحلقات المضلعات.

يوجد تحقق واحد مستقل عن هذا الإعداد: الإحداثيات غير المتناهية (`NaN`, `Inf`) تُرفض دائمًا، لأنه لا يمكن تمثيلها كأرقام JSON.