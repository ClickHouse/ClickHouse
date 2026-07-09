---
description: 'وثائق Geohash'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'دوال التعامل مع Geohash'
doc_type: 'reference'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) هو نظام ترميز جغرافي يقسّم سطح الأرض إلى مناطق شبكية ويشفّر كل خلية إلى سلسلة قصيرة من الأحرف والأرقام. وهو بنية بيانات هرمية، لذا كلما زاد طول سلسلة geohash، زادت دقة الموقع الجغرافي.

إذا كنت بحاجة إلى تحويل الإحداثيات الجغرافية يدويًا إلى سلاسل geohash، فيمكنك استخدام [geohash.org](http://geohash.co/)

<div id="geohashencode">
  ## geohashEncode
</div>

يُرمِّز خط العرض وخط الطول في صورة سلسلة [geohash](#geohash).

**صيغة**

```sql
geohashEncode(longitude, latitude, [precision])
```

**قيم الإدخال**

* `longitude` — جزء خط الطول من الإحداثي المراد ترميزه. قيمة عائمة ضمن النطاق `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude` — جزء خط العرض من الإحداثي المراد ترميزه. قيمة عائمة ضمن النطاق `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` (اختياري) — طول السلسلة المُرمَّزة الناتجة. القيمة الافتراضية هي `12`. عدد صحيح ضمن النطاق `[1, 12]`. [Int8](../../data-types/int-uint.md).

:::note

* يجب أن تكون جميع معلمات الإحداثيات من النوع نفسه: إما `Float32` أو `Float64`.
* بالنسبة إلى المعلمة `precision`، فإن أي قيمة أقل من `1` أو أكبر من `12` تُحوَّل بصمت إلى `12`.
  :::

**القيم المرتجعة**

* سلسلة أبجدية رقمية تمثل الإحداثي المُرمَّز (يُستخدم إصدار معدّل من أبجدية ترميز base32). [String](../../data-types/string.md).

**مثال**

```sql title="Query"
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```text title="Response"
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

<div id="geohashdecode">
  ## geohashDecode
</div>

يفك ترميز أي سلسلة مُرمَّزة باستخدام [geohash](#geohash) إلى خط الطول وخط العرض.

**الصيغة**

```sql
geohashDecode(hash_str)
```

**قيم الإدخال**

* `hash_str` — سلسلة مرمَّزة باستخدام Geohash.

**القيم المعادة**

* Tuple `(longitude, latitude)` من قيم `Float64` تمثل خط الطول وخط العرض. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**مثال**

```sql
SELECT geohashDecode('ezs42') AS res;
```

```text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

<div id="geohashesinbox">
  ## geohashesInBox
</div>

يعيد مصفوفة من السلاسل المُرمَّزة باستخدام [geohash](#geohash) وبالدقة المحددة، تقع داخل حدود الصندوق المحدد وتتقاطع معها، وهي في الأساس شبكة ثنائية الأبعاد مُسطَّحة في صورة مصفوفة.

**صيغة**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**الوسيطات**

* `longitude_min` — الحد الأدنى لخط الطول. النطاق: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_min` — الحد الأدنى لخط العرض. النطاق: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `longitude_max` — الحد الأقصى لخط الطول. النطاق: `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_max` — الحد الأقصى لخط العرض. النطاق: `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` — دقة geohash. النطاق: `[1, 12]`. [UInt8](../../data-types/int-uint.md).

:::note
يجب أن تكون جميع معلمات الإحداثيات من النوع نفسه: إما `Float32` أو `Float64`.
:::

**القيم المُعادة**

* مصفوفة من سلاسل geohash بطول يساوي قيمة precision وتغطي المنطقة المحددة، ويجب عدم الاعتماد على ترتيب العناصر. [Array](../../data-types/array.md)([String](../../data-types/string.md)).
* `[]` - مصفوفة فارغة إذا لم تكن القيم الدنيا لخط العرض وخط الطول أقل من القيم القصوى المناظرة لها.

:::note
تُطلق الدالة استثناءً إذا تجاوز طول المصفوفة الناتجة 10&#39;000&#39;000 عنصر.
:::

**مثال**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```