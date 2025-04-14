---
slug: /en/sql-reference/functions/geo/geohash
sidebar_label: Geohash
title: "Functions for Working with Geohash"
---

## Geohash

[Geohash](https://en.wikipedia.org/wiki/Geohash) is the geocode system, which subdivides Earth’s surface into buckets of grid shape and encodes each cell into a short string of letters and digits. It is a hierarchical data structure, so the longer the geohash string is, the more precise the geographic location will be.

If you need to manually convert geographic coordinates to geohash strings, you can use [geohash.org](http://geohash.org/).

## geohashEncode

Encodes latitude and longitude as a [geohash](#geohash)-string.

**Syntax**

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Input values**

- `longitude` — Longitude part of the coordinate you want to encode. Floating in range`[-180°, 180°]`. [Float](../../data-types/float.md). 
- `latitude` — Latitude part of the coordinate you want to encode. Floating in range `[-90°, 90°]`. [Float](../../data-types/float.md).
- `precision` (optional) — Length of the resulting encoded string. Defaults to `12`. Integer in the range `[1, 12]`. [Int8](../../data-types/int-uint.md).

:::note
- All coordinate parameters must be of the same type: either `Float32` or `Float64`.
- For the `precision` parameter, any value less than `1` or greater than `12` is silently converted to `12`.
:::

**Returned values**

- Alphanumeric string of the encoded coordinate (modified version of the base32-encoding alphabet is used). [String](../../data-types/string.md).

**Example**

Query:

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

Result:

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode

Decodes any [geohash](#geohash)-encoded string into longitude and latitude.

**Syntax**

```sql
geohashDecode(hash_str)
```

**Input values**

- `hash_str` — Geohash-encoded string.

**Returned values**

- Tuple `(longitude, latitude)` of `Float64` values of longitude and latitude. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**Example**

``` sql
SELECT geohashDecode('ezs42') AS res;
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geohashesInBox

Returns an array of [geohash](#geohash)-encoded strings of given precision that fall inside and intersect boundaries of given box, basically a 2D grid flattened into array.

**Syntax**

``` sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**Arguments**

- `longitude_min` — Minimum longitude. Range: `[-180°, 180°]`. [Float](../../data-types/float.md).
- `latitude_min` — Minimum latitude. Range: `[-90°, 90°]`. [Float](../../data-types/float.md).
- `longitude_max` — Maximum longitude. Range: `[-180°, 180°]`. [Float](../../data-types/float.md).
- `latitude_max` — Maximum latitude. Range: `[-90°, 90°]`. [Float](../../data-types/float.md).
- `precision` — Geohash precision. Range: `[1, 12]`. [UInt8](../../data-types/int-uint.md).

:::note    
All coordinate parameters must be of the same type: either `Float32` or `Float64`.
:::

**Returned values**

- Array of precision-long strings of geohash-boxes covering provided area, you should not rely on order of items. [Array](../../data-types/array.md)([String](../../data-types/string.md)).
- `[]` - Empty array if minimum latitude and longitude values aren’t less than corresponding maximum values.

:::note    
Function throws an exception if resulting array is over 10’000’000 items long.
:::

**Example**

Query:

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

Result:

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```
