---
description: 'توثيق لترميز Mapbox Vector Tiles'
sidebar_label: 'Mapbox Vector Tiles'
sidebar_position: 65
slug: /sql-reference/functions/geo/mvt
title: 'دوال ترميز Mapbox Vector Tiles'
doc_type: 'مرجع'
---

<div id="overview">
  ## نظرة عامة
</div>

تُعد [Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec)‏ (MVT) بلاطات مُرمَّزة بصيغة Protobuf تعرضها عملاء خرائط الويب
مثل MapLibre وMapbox GL بشكل أصلي. ويمكن لـ ClickHouse إنشاء هذه البلاطات بالكامل في SQL باستخدام زوج من
الدوال المتعاونة:

* `MVTEncodeGeom` — دالة قياسية تُسقِط شكلاً هندسيًا إلى فضاء البكسلات المحلي للبلاطة ضمن بلاطة slippy-map، وتقصّه
  وفق حدود البلاطة.
* `MVTEncode` — دالة تجميعية تجمع الأشكال الهندسية المُسقطة لمجموعة ما في بايتات ثنائية لبلاطة
  أحادية الطبقة.

تعيد دالتا المساعدة `MVTBoundingBox` و`MVTBoundingBoxMercator` مربع الإحاطة الخاص ببلاطة بحيث يمكن
قصر الصفوف عليه ضمن عبارة `WHERE` باستخدام فهرس.

تدعم هذه الدوال الأشكال الهندسية من نوع النقطة والخط والمضلع، بما في ذلك النوع `Geometry` والأنواع الجغرافية المحددة (`Point`,
`LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon`).

البايتات الناتجة تمثل بلاطة كاملة يمكن إرجاعها مباشرة عبر واجهة HTTP باستخدام `FORMAT RawBLOB`.

تحاكي هذه الدوال سير عمل PostGIS، وهي متاحة أيضًا تحت أسماء PostGIS الخاصة بها كأسماء مستعارة: `ST_AsMVTGeom`
لـ `MVTEncodeGeom` و`ST_AsMVT` لـ `MVTEncode`.

<div id="mvtencodegeom">
  ## MVTEncodeGeom
</div>

يُسقِط شكلاً هندسياً مُعطىً بالإحداثيات الجغرافية (خط الطول/خط العرض) إلى فضاء البكسلات المحلي للبلاطة في
`slippy-map` والمُحدَّدة بواسطة `zoom` و`tile_x` و`tile_y`، ثم يطابقه مع شبكة البكسلات الصحيحة، ويقصّه إلى حدود البلاطة،
ويُرجع الشكل الهندسي في `tile-space`.

الإسقاط هنا هو Web Mercator على كامل نطاق إحداثيات `UInt32`. وتكون الإحداثيات المُعادة ذات أصل عند
الزاوية العلوية اليسرى من البلاطة، مع اتجاه المحور y إلى الأسفل، وهو اصطلاح الإحداثيات المعتمد في صيغة Mapbox Vector
Tile، لذا يمكن تمرير النتيجة مباشرةً إلى `MVTEncode`. وتُقرَّب الإحداثيات إلى بكسلات كاملة، لذا فإن التجميع حسب
`MVTEncodeGeom` يدمج الأشكال الهندسية الواقعة على الشبكة نفسها في عنقود واحد.

عندما يكون `clip` مفعّلاً (وهو الإعداد الافتراضي)، يُقصّ الشكل الهندسي إلى البلاطة بعد توسيعها بمقدار `buffer` بكسل (أي ضمن النطاق
`[-buffer, extent + buffer]` على كل محور)؛ وأي شكل هندسي يقع بالكامل خارجها يصبح `NULL`. وهذا مماثل لـ
PostGIS `ST_AsMVTGeom`.

تُقيَّد إحداثيات Polygon ضمن نطاق `2^30` قبل التحقق — وهو يطابق تماماً امتداد البكسلات للعالم كله عند
`zoom` 18 و`extent` 4096 — لذلك، في البلاطات الواقعية، يجري التحقق من صحة الشكل الهندسي من دون قصّه مطلقاً، ولا يؤثر هذا القيد إلا
في الأشكال الهندسية الموضوعة عند القيم القصوى لـ `zoom` أو `extent`.

يعتمد نوع الشكل الهندسي الناتج على نوع الإدخال: فالقيمة `Point` تُرجع `Point`؛ و`LineString` أو `MultiLineString` تُرجع
`MultiLineString`؛ و`Ring` أو `Polygon` أو `MultiPolygon` تُرجع `MultiPolygon` (إذ قد يؤدي القص إلى تقسيم الشكل الهندسي إلى
عدة أجزاء).

**البنية**

```sql
MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])
```

**المعاملات**

* `geometry` — كائن Geometry بدرجات خط الطول/خط العرض. يُقيَّد خط الطول إلى `[-180, 180]`، ويُقيَّد خط العرض إلى نطاق Web Mercator `[-85.05112878, 85.05112878]`. [`Point`](../../data-types/geo.md) / [`LineString`](../../data-types/geo.md) / [`MultiLineString`](../../data-types/geo.md) / [`Ring`](../../data-types/geo.md) / [`Polygon`](../../data-types/geo.md) / [`MultiPolygon`](../../data-types/geo.md) / [`Geometry`](../../data-types/geo.md).
* `zoom` — مستوى التكبير في slippy-map، ضمن النطاق `[0, 32]`. [`UInt8`](../../data-types/int-uint.md).
* `tile_x` — فهرس عمود البلاطة، ضمن النطاق `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
* `tile_y` — فهرس صف البلاطة، ضمن النطاق `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
* `extent` — الامتداد الاختياري للبلاطة بالبكسل لكل ضلع، ضمن النطاق `[1, 2147483647]`. القيمة الافتراضية هي `4096`، وهي القيمة الافتراضية لـ Mapbox Vector Tile. [`UInt32`](../../data-types/int-uint.md).
* `buffer` — buffer قصّ اختياري بالبكسل، ضمن النطاق `[0, 2147483647]`. القيمة الافتراضية هي `1`. [`UInt32`](../../data-types/int-uint.md).
* `clip` — flag اختياري؛ عندما تكون قيمته غير صفرية (وهو الافتراضي)، يُقصّ الشكل الهندسي إلى حدود البلاطة مع الـ buffer. [`UInt8`](../../data-types/int-uint.md).

**القيمة المُعادة**

يعيد الشكل الهندسي في `tile-space`، أو `NULL` إذا تم قصّه بالكامل. [`Geometry`](../../data-types/geo.md).

**مثال**

```sql
SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335) AS pixel
```

```text
┌─pixel──────┐
│ (124,3384) │
└────────────┘
```

<div id="mvtencode">
  ## MVTEncode
</div>

يُرمِّز مجموعة من المعالم إلى طبقة ثنائية بتنسيق Mapbox Vector Tile. وهذا هو المقابل التجميعي للدالة القياسية
`MVTEncodeGeom`. يتحول كل صف إدخال إلى معلم واحد؛ وتكون الأشكال الهندسية المدعومة هي النقاط والخطوط والمضلعات.

الوسيط `geometry` هو `Geometry` بإحداثيات tile-space، ويُنتَج عادةً بواسطة `MVTEncodeGeom`. وتُتخطى الصفوف التي تكون
هندستها `NULL` (على سبيل المثال، إذا اقتُطعت بواسطة `MVTEncodeGeom`). أما الوسيط الاختياري `properties` فهو
Tuple مُسمّى، وتصبح أسماء عناصره مفاتيح سمات المعلم، بينما تحدد أنواع عناصره أنواع القيم في vector بلاطة.

النتيجة هي بايتات خام لبلاطة أحادية الطبقة. وتنتج المجموعة الفارغة بلاطة فارغة. وهذا هو النظير لـ
PostGIS `ST_AsMVT`.

**البنية**

```sql
MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])
```

**المعلمات**

* `layer_name` — اسم طبقة البلاطة المتجهة. [`String`](../../data-types/string.md).
* `extent` — امتداد البلاطة بالبكسل لكل ضلع، ضمن النطاق `[1, 2147483647]`. القيمة الافتراضية هي `4096`. [`UInt32`](../../data-types/int-uint.md).
* `feature_id_name` — اسم اختياري لعنصر عدد صحيح غير موقّع ضمن tuple ‏`properties` ليُخرَج باعتباره `id` للمعلم في MVT (من النوع `UInt64`) بدلًا من إخراجه كوسم. تُرفَض الأعداد الصحيحة الموقَّعة. وإذا كانت قيمة `id` هي `NULL`، فسيُحذَف لهذا المعلم. المعلمات موضعية، لذا يجب تمرير `extent` لاستخدامه. [`String`](../../data-types/string.md).
* `stringify_unsupported` — علامة اختيارية (`0`/`1`، والقيمة الافتراضية `0`)؛ عند ضبطها على `1`، تُرمَّز أنواع الخصائص غير المدعومة مباشرةً (مثل الأعداد الصحيحة الكبيرة و`UUID` و`Decimal`) على أنها `string_value` نصية بدلًا من إصدار خطأ. [`UInt8`](../../data-types/int-uint.md).

**الوسائط**

* `geometry` — الهندسة في tile-space، على سبيل المثال من `MVTEncodeGeom`. [`Geometry`](../../data-types/geo.md).
* `properties` — named tuple اختياري لسمات المعلم. تتحول أسماء العناصر إلى مفاتيح السمات. [`Tuple`](../../data-types/tuple.md).

**القيمة المعادة**

يعيد المحتوى الثنائي لبلاطة Mapbox Vector Tile أحادية الطبقة. [`String`](../../data-types/string.md).

<div id="property-types">
  ### أنواع الخصائص
</div>

يُرمَّز كل عنصر خاصية باعتباره متغيّر `Value` في Mapbox Vector Tile المطابق لنوعه في ClickHouse:

| نوع ClickHouse                                                 | نوع قيمة Vector Tile |
| -------------------------------------------------------------- | -------------------- |
| `String` / `FixedString`                                       | `string_value`       |
| `Float32` / `BFloat16`                                         | `float_value`        |
| `Float64`                                                      | `double_value`       |
| `Bool`                                                         | `bool_value`         |
| `Int8` / `Int16` / `Int32` / `Int64` / `Date32`                | `sint_value`         |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` / `Date` / `DateTime` | `uint_value`         |

يمكن التفاف الأنواع داخل `Nullable` و/أو `LowCardinality`. تؤدي القيمة `NULL` إلى حذف تلك السمة من المعلم، لأن تنسيق
Vector Tile لا يحتوي على قيمة null. وأي نوع خاصية آخر يؤدي إلى استثناء، ما لم يكن `stringify_unsupported` مفعّلًا، وفي
هذه الحالة يُرمَّز على هيئة `string_value` نصية.

تُخزَّن قيم الخصائص المتطابقة في مجمّع القيم المشترك للطبقة، لذا فإن القيمة التي تظهر في كثير من المعالم
لا تُخزَّن إلا مرة واحدة.

<div id="naming-the-properties-tuple">
  ### تسمية tuple الخصائص
</div>

يجب أن تتضمن tuple الخصائص أسماءً صريحة للعناصر. ولا تُنقَل الأسماء المستعارة للأعمدة داخل `tuple(...)` إلى أسماء عناصر
الـ tuple، لذا سمِّ العناصر باستخدام cast:

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

<div id="clustering">
  ### تجميع عنقودي
</div>

يُعبَّر عن التجميع العنقودي في SQL، وليس عبر الدالة. ولأن `MVTEncodeGeom` يقرّب القيم إلى بكسلات كاملة، فإن التجميع بحسب
الهندسة على مستوى البكسل يدمج الأشكال الهندسية المتطابقة؛ نفّذ التجميع على مستوى المجموعة في استعلام فرعي، ثم مرّر صفًا واحدًا لكل عنقود إلى
`MVTEncode`:

```sql
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1;
```

يتطلّب التجميع على قيمة `Geometry` تعيين `allow_suspicious_types_in_group_by = 1`، لأن التجميع حسب النوع
`Geometry` المستند إلى `Variant` مقيّد افتراضيًا. احذف `GROUP BY` الداخلي (و`count()`) لإخراج معلم واحد لكل صف إدخال
بدلًا من المعالم المجمّعة في عناقيد.

<div id="mvtboundingbox">
  ## MVTBoundingBox
</div>

تُرجِع مربع الإحاطة الجغرافي لبلاطة `slippy-map` المحددة بواسطة `zoom` و`tile_x` و`tile_y` على هيئة tuple
`(min_lon, min_lat, max_lon, max_lat)` بالدرجات.

استخدمها لقصر الصفوف على بلاطة معيّنة عند التصفية مباشرةً على العمودين `longitude` و`latitude` — بحيث يمكن استخدام المفتاح الأساسي أو
الفهرس على هذين العمودين — بدلاً من إعادة حساب إسقاط Web Mercator لكل صف. يوسّع المعامل الاختياري `margin`
مربع الإحاطة من كل جانب بمقدار هذه النسبة من حجم البلاطة؛ اضبطه على `buffer / extent` لتغطية مخزن القص المؤقت الخاص بـ
`MVTEncodeGeom`.

**البنية**

```sql
MVTBoundingBox(zoom, tile_x, tile_y[, margin])
```

**المعاملات**

* `zoom` — مستوى التكبير في `slippy-map`، ضمن النطاق `[0, 32]`. [`UInt8`](../../data-types/int-uint.md).
* `tile_x` — فهرس عمود البلاطة، ضمن النطاق `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
* `tile_y` — فهرس صف البلاطة، ضمن النطاق `[0, 2^zoom - 1]`. [`UInt32`](../../data-types/int-uint.md).
* `margin` — جزء اختياري من حجم البلاطة لتوسيع الصندوق من جميع الجوانب. القيمة الافتراضية هي `0`. [`Float64`](../../data-types/float.md).

**القيمة المُعادة**

يعيد مربع الإحاطة للبلاطة على هيئة tuple `(min_lon, min_lat, max_lon, max_lat)` بالدرجات. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**مثال**

```sql
SELECT MVTBoundingBox(0, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────────────────────────┐
│ (-180,-85.05112877980659,180,85.05112877980659)  │
└──────────────────────────────────────────────────┘
```

<div id="mvtboundingboxmercator">
  ## MVTBoundingBoxMercator
</div>

النظير في Web Mercator للدالة `MVTBoundingBox`. يعيد
مربع الإحاطة للبلاطة في فضاء إحداثيات Web Mercator الكامل من نوع `UInt32` والمستخدَم داخليًا بواسطة `MVTEncodeGeom`، على هيئة tuple
`(min_x, min_y, max_x, max_y)`. يزداد المحور y نزولًا (الشمال في الأعلى). وهو مخصص للجداول التي تُجسِّد
أعمدة إحداثيات Mercator وتُفهرِسها بدلًا من `longitude`/`latitude`.

**البنية**

```sql
MVTBoundingBoxMercator(zoom, tile_x, tile_y[, margin])
```

**الوسيطات**

نفس [`MVTBoundingBox`](#mvtboundingbox).

**القيمة المعادة**

يُرجع مربع الإحاطة للبلاطة على هيئة tuple `(min_x, min_y, max_x, max_y)` بإحداثيات Web Mercator. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**مثال**

```sql
SELECT MVTBoundingBoxMercator(1, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648)  │
└──────────────────────────────┘
```

<div id="restricting-rows-to-a-tile">
  ## حصر الصفوف ضمن بلاطة
</div>

يجب ألا تحتوي البلاطة إلا على الهندسة التابعة لها. وأفضل طريقة للتعبير عن ذلك هي خطوتان متكاملتان: شرط مربع إحاطة منخفض الكلفة
يستخدم الفهرس في عبارة `WHERE` (للأداء)، وعملية الاقتصاص في `MVTEncodeGeom` (لضمان الصحة).
تستبعد عملية الاقتصاص أي هندسة تقع خارج البلاطة، لذلك حتى شرط مربع إحاطة متساهل لا يمكنه أن يسرّب هندسة من خارج البلاطة إلى
النتيجة.

```sql
WITH
    1 AS buffer,
    4096 AS extent,
    MVTBoundingBox({z:UInt8}, {x:UInt32}, {y:UInt32}, buffer / extent) AS bounding_box   -- margin matches the clip buffer
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
    FROM points
    WHERE lon BETWEEN bounding_box.1 AND bounding_box.3 AND lat BETWEEN bounding_box.2 AND bounding_box.4   -- index-using prefilter
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1
```

شرط المربّع المحيط ليس سوى ترشيح أولي تقريبي؛ إذ يُطبَّق الحدّ الدقيق للبلاطة عبر الاقتصاص في
`MVTEncodeGeom`. مرّر `clip => false` (الوسيطة السابعة) إلى `MVTEncodeGeom` لتعطيل الاقتصاص والاعتماد على
شرط `WHERE` وحده.

<div id="serving-tiles-over-http">
  ## تقديم البلاطات عبر HTTP
</div>

لا يعرِض ClickHouse نقطة نهاية للبلاطات افتراضيًا، إذ لا تقبل واجهة HTTP سوى الاستعلامات على `/`. ويضيف المشغّل عنوان URL نظيفًا
`/tile/{z}/{x}/{y}` باستخدام [معالج استعلام مُعرّف مسبقًا](/ar/interfaces/http) في
تهيئة الخادم. ويستخدم `url` الخاص بالمعالج صيغة `regex:` لالتقاط مقاطع المسار، وربطها بمعلمات
الاستعلام، ثم إرجاع البايتات باستخدام `FORMAT RawBLOB`.

في أبسط الحالات، يحتوي الجدول على عمود `Geometry`، ويخدم المعالج معلمًا واحدًا لكل صف — إذ يُسقِط `MVTEncodeGeom`
كل شكل هندسي على البلاطة المطلوبة ويقصّه، لذلك تُستبعَد الصفوف الواقعة خارج البلاطة تلقائيًا:

```xml
<http_handlers>
    <rule>
        <methods>GET</methods>
        <url><![CDATA[regex:/tile/(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)]]></url>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                SELECT MVTEncode('shapes')(
                    MVTEncodeGeom(geom, {z:UInt8}, {x:UInt32}, {y:UInt32}),
                    tuple(id, name)::Tuple(id UInt32, name String))
                FROM shapes
                FORMAT RawBLOB
            </query>
            <content_type>application/vnd.mapbox-vector-tile</content_type>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

هنا `shapes` هو جدول يحتوي على عمود `geom Geometry` (أي مزيج من النقاط والخطوط والمضلعات). ويُرجع `GET /tile/10/550/335`
البلاطة المُرمَّزة.

بالنسبة إلى بيانات النقاط، يعمل هذا بالكفاءة نفسها مع أعمدة `longitude`/`latitude` العادية، من خلال إنشاء النقطة مباشرةً باستخدام
`MVTEncodeGeom((lon, lat)::Point, …)`. ولتنفيذ تجميع عنقودي للمعالم المتطابقة، أو لإضافة ترشيح مسبق بمربع الإحاطة يستخدم الفهرس
للجداول الكبيرة، وسّع الاستعلام الداخلي كما هو موضح في [Clustering](#clustering) و
[Restricting rows to a tile](#restricting-rows-to-a-tile).

<div id="limitations">
  ## القيود
</div>

* يقيّد إسقاط Web Mercator خط العرض عند `±85.05112878°`، ولا يدعم المدخلات التي تعبر خط الزوال المقابل.