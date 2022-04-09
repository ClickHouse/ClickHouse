---
sidebar_label: Geohash
---

# Functions for Working with Geohash {#geohash}

[Geohash](https://en.wikipedia.org/wiki/Geohash) is the geocode system, which subdivides Earth’s surface into buckets of grid shape and encodes each cell into a short string of letters and digits. It is a hierarchical data structure, so the longer is the geohash string, the more precise is the geographic location.

If you need to manually convert geographic coordinates to geohash strings, you can use [geohash.org](http://geohash.org/).

## geohashEncode {#geohashencode}

Encodes latitude and longitude as a [geohash](#geohash)-string.

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Input values**

-   longitude - longitude part of the coordinate you want to encode. Floating in range`[-180°, 180°]`
-   latitude - latitude part of the coordinate you want to encode. Floating in range `[-90°, 90°]`
-   precision - Optional, length of the resulting encoded string, defaults to `12`. Integer in range `[1, 12]`. Any value less than `1` or greater than `12` is silently converted to `12`.

**Returned values**

-   alphanumeric `String` of encoded coordinate (modified version of the base32-encoding alphabet is used).

**Example**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

Decodes any [geohash](#geohash)-encoded string into longitude and latitude.

**Input values**

-   encoded string - geohash-encoded string.

**Returned values**

-   (longitude, latitude) - 2-tuple of `Float64` values of longitude and latitude.

**Example**

``` sql
SELECT geohashDecode('ezs42') AS res;
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geohashesInBox {#geohashesinbox}

Returns an array of [geohash](#geohash)-encoded strings of given precision that fall inside and intersect boundaries of given box, basically a 2D grid flattened into array.

**Syntax**

``` sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**Arguments**

-   `longitude_min` — Minimum longitude. Range: `[-180°, 180°]`. Type: [Float](../../../sql-reference/data-types/float.md).
-   `latitude_min` — Minimum latitude. Range: `[-90°, 90°]`. Type: [Float](../../../sql-reference/data-types/float.md).
-   `longitude_max` — Maximum longitude. Range: `[-180°, 180°]`. Type: [Float](../../../sql-reference/data-types/float.md).
-   `latitude_max` — Maximum latitude. Range: `[-90°, 90°]`. Type: [Float](../../../sql-reference/data-types/float.md).
-   `precision` — Geohash precision. Range: `[1, 12]`. Type: [UInt8](../../../sql-reference/data-types/int-uint.md).

:::note    
All coordinate parameters must be of the same type: either `Float32` or `Float64`.
:::

**Returned values**

-   Array of precision-long strings of geohash-boxes covering provided area, you should not rely on order of items.
-   `[]` - Empty array if minimum latitude and longitude values aren’t less than corresponding maximum values.

Type: [Array](../../../sql-reference/data-types/array.md)([String](../../../sql-reference/data-types/string.md)).

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

[Original article](https://clickhouse.com/docs/en/sql-reference/functions/geo/geohash) <!--hide-->
