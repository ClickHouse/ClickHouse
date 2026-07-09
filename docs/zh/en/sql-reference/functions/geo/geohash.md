---
description: 'Geohash 相关文档'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'Geohash 相关函数'
doc_type: 'reference'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) 是一种地理编码系统，它将地球表面划分为网格状的区域，并将每个单元编码为由字母和数字组成的短字符串。它是一种层次化数据结构，因此 geohash 字符串越长，地理位置就越精确。

如果需要手动将地理坐标转换为 geohash 字符串，可以使用 [geohash.org](http://geohash.co/)

<div id="geohashencode">
  ## geohashEncode
</div>

将纬度和经度编码为 [geohash](#geohash) 字符串。

**语法**

```sql
geohashEncode(longitude, latitude, [precision])
```

**输入值**

* `longitude` — 要编码的坐标中的经度部分。浮点数，取值范围为 `[-180°, 180°]`。[Float](../../data-types/float.md)。
* `latitude` — 要编码的坐标中的纬度部分。浮点数，取值范围为 `[-90°, 90°]`。[Float](../../data-types/float.md)。
* `precision` (可选) — 编码结果字符串的长度。默认为 `12`。整数，取值范围为 `[1, 12]`。[Int8](../../data-types/int-uint.md)。

:::note

* 所有坐标参数必须为相同类型：`Float32` 或 `Float64`。
* 对于 `precision` 参数，任何小于 `1` 或大于 `12` 的值都会静默转换为 `12`。
  :::

**返回值**

* 编码后坐标的字母数字字符串 (使用修改版的 base32 编码字母表) 。[String](../../data-types/string.md)。

**示例**

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

将任意 [geohash](#geohash) 编码字符串解码为经度和纬度。

**语法**

```sql
geohashDecode(hash_str)
```

**输入值**

* `hash_str` — Geohash 编码的字符串。

**返回值**

* 由经度和纬度的 `Float64` 值构成的 Tuple `(longitude, latitude)`。[Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**示例**

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

返回一个数组，其中包含按给定精度编码的 [geohash](#geohash) 字符串；这些字符串位于给定框内并与其边界相交，本质上就是将二维网格展平为数组。

**语法**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**参数**

* `longitude_min` — 最小经度。范围：`[-180°, 180°]`。[Float](../../data-types/float.md)。
* `latitude_min` — 最小纬度。范围：`[-90°, 90°]`。[Float](../../data-types/float.md)。
* `longitude_max` — 最大经度。范围：`[-180°, 180°]`。[Float](../../data-types/float.md)。
* `latitude_max` — 最大纬度。范围：`[-90°, 90°]`。[Float](../../data-types/float.md)。
* `precision` — geohash 精度。范围：`[1, 12]`。[UInt8](../../data-types/int-uint.md)。

:::note
所有坐标参数必须为相同类型：`Float32` 或 `Float64`。
:::

**返回值**

* 由覆盖给定区域的、长度为指定精度的 geohash 字符串组成的数组；不应依赖元素的顺序。[Array](../../data-types/array.md)([String](../../data-types/string.md))。
* `[]` - 如果最小纬度和经度值不小于对应的最大值，则返回空数组。

:::note
如果结果数组超过 10&#39;000&#39;000 项，函数会抛出异常。
:::

**示例**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```