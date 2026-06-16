#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeVariant.h>

namespace DB
{

void registerDataTypeDomainGeo(DataTypeFactory & factory)
{
    // Custom type for point represented as its coordinates stored as Tuple(Float64, Float64)
    factory.registerSimpleDataTypeCustom("Point", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Tuple(Float64, Float64)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePointName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(
ClickHouse supports data types for representing geographical objects — locations, lands, etc.

**See Also**
- [Representing simple geographical features](https://en.wikipedia.org/wiki/GeoJSON).

## Point {#point}

`Point` is represented by its X and Y coordinates, stored as a [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Example**

```sql title="Query"
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```

```text title="Response"
┌─p───────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└─────────┴───────────────┘
```

## Ring {#ring}

`Ring` is a simple polygon without holes stored as an array of points: [Array](array.md)([Point](#point)).

**Example**

```sql title="Query"
CREATE TABLE geo_ring (r Ring) ENGINE = Memory();
INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT r, toTypeName(r) FROM geo_ring;
```

```text title="Response"
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ Ring          │
└───────────────────────────────┴───────────────┘
```

## LineString {#linestring}

`LineString` is a line stored as an array of points: [Array](array.md)([Point](#point)).

**Example**

```sql title="Query"
CREATE TABLE geo_linestring (l LineString) ENGINE = Memory();
INSERT INTO geo_linestring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT l, toTypeName(l) FROM geo_linestring;
```

```text title="Response"
┌─l─────────────────────────────┬─toTypeName(l)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ LineString    │
└───────────────────────────────┴───────────────┘
```

## MultiLineString {#multilinestring}

`MultiLineString` is multiple lines stored as an array of `LineString`: [Array](array.md)([LineString](#linestring)).

**Example**

```sql title="Query"
CREATE TABLE geo_multilinestring (l MultiLineString) ENGINE = Memory();
INSERT INTO geo_multilinestring VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
SELECT l, toTypeName(l) FROM geo_multilinestring;
```

```text title="Response"
┌─l───────────────────────────────────────────────────┬─toTypeName(l)───┐
│ [[(0,0),(10,0),(10,10),(0,10)],[(1,1),(2,2),(3,3)]] │ MultiLineString │
└─────────────────────────────────────────────────────┴─────────────────┘
```

## Polygon {#polygon}

`Polygon` is a polygon with holes stored as an array of rings: [Array](array.md)([Ring](#ring)). First element of outer array is the outer shape of polygon and all the following elements are holes.

**Example**

This is a polygon with one hole:

```sql title="Query"
CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory();
INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT pg, toTypeName(pg) FROM geo_polygon;
```

```text title="Response"
┌─pg────────────────────────────────────────────────────────────┬─toTypeName(pg)─┐
│ [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]] │ Polygon        │
└───────────────────────────────────────────────────────────────┴────────────────┘
```

## MultiPolygon {#multipolygon}

`MultiPolygon` consists of multiple polygons and is stored as an array of polygons: [Array](array.md)([Polygon](#polygon)).

**Example**

This multipolygon consists of two separate polygons — the first one without holes, and the second with one hole:

```sql title="Query"
CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory();
INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT mpg, toTypeName(mpg) FROM geo_multipolygon;
```

```text title="Response"
┌─mpg─────────────────────────────────────────────────────────────────────────────────────────────┬─toTypeName(mpg)─┐
│ [[[(0,0),(10,0),(10,10),(0,10)]],[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]] │ MultiPolygon    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┘
```

## Geometry {#geometry}

`Geometry` is a common type for all the types above. It is equivalent to a Variant of those types.

**Example**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo (geom Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ((1, 2));
SELECT * FROM geo;
```

```text title="Response"
┌─geom──┐
│ (1,2) │
└───────┘
```

<!-- -->

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();

CREATE TABLE IF NOT EXISTS geo (geom String, id Int) ENGINE = Memory();
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 1);
INSERT INTO geo VALUES ('POINT(0 0)', 2);
INSERT INTO geo VALUES ('MULTIPOLYGON(((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 3);
INSERT INTO geo VALUES ('LINESTRING(1 0,10 0,10 10,0 10,1 0)', 4);
INSERT INTO geo VALUES ('MULTILINESTRING((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 5);
INSERT INTO geo_dst SELECT readWKT(geom) FROM geo ORDER BY id;

SELECT * FROM geo_dst;
```

```text title="Response"
┌─geom─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]]                                            │
│ (0,0)                                                                                                            │
│ [[[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]] │
│ [(1,0),(10,0),(10,10),(0,10),(1,0)]                                                                              │
│ [[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]]                                            │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Related Content {#related-content}

- [Exploring massive, real-world data sets: 100+ Years of Weather Records in ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
)DOCS_MD",
        .syntax = "Point",
        .examples = {},
        .related = {"Ring", "Polygon", "LineString"},
    });

    // Custom type for simple line which consists from several segments.
    factory.registerSimpleDataTypeCustom("LineString", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeLineStringName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(A `LineString` is a line consisting of several segments, stored as an `Array(Point)`.)DOCS_MD",
        .syntax = "LineString",
        .examples = {},
        .related = {"Point"},
    });

    // Custom type for multiple lines stored as Array(LineString)
    factory.registerSimpleDataTypeCustom("MultiLineString", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(LineString)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeMultiLineStringName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(A `MultiLineString` is multiple lines, stored as an `Array(LineString)`.)DOCS_MD",
        .syntax = "MultiLineString",
        .examples = {},
        .related = {"Point"},
    });

    // Custom type for simple polygon without holes stored as Array(Point)
    factory.registerSimpleDataTypeCustom("Ring", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeRingName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(A `Ring` is a simple polygon without holes, stored as an `Array(Point)` (the sequence of vertices).)DOCS_MD",
        .syntax = "Ring",
        .examples = {},
        .related = {"Point"},
    });

    // Custom type for polygon with holes stored as Array(Ring)
    // First element of outer array is outer shape of polygon and all the following are holes
    factory.registerSimpleDataTypeCustom("Polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Ring)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePolygonName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(A `Polygon` is a polygon with holes, stored as an `Array(Ring)`. The first element of the outer array is the outer shape of the polygon and all subsequent elements are holes.)DOCS_MD",
        .syntax = "Polygon",
        .examples = {},
        .related = {"Point"},
    });

    // Custom type for multiple polygons with holes stored as Array(Polygon)
    factory.registerSimpleDataTypeCustom("MultiPolygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(Polygon)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeMultiPolygonName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(A `MultiPolygon` consists of multiple polygons, stored as an `Array(Polygon)`.)DOCS_MD",
        .syntax = "MultiPolygon",
        .examples = {},
        .related = {"Point"},
    });

    factory.registerSimpleDataTypeCustom("Geometry", []
    {
        auto point_type = DataTypeFactory::instance().get(DataTypePointName().getName());
        auto linestring_type = DataTypeFactory::instance().get(DataTypeLineStringName().getName());
        auto polygon_type = DataTypeFactory::instance().get(DataTypePolygonName().getName());
        auto multipolygon_type = DataTypeFactory::instance().get(DataTypeMultiPolygonName().getName());
        auto ring_type = DataTypeFactory::instance().get(DataTypeRingName().getName());
        auto multi_linestring_type = DataTypeFactory::instance().get(DataTypeMultiLineStringName().getName());

        auto variant_type = std::make_shared<DataTypeVariant>(std::vector{point_type, linestring_type, polygon_type, multipolygon_type, ring_type, multi_linestring_type});

        return std::make_pair(variant_type,
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeGeometryName>()));
    }, DataTypeFactory::Case::Sensitive, Documentation{
        .description = R"DOCS_MD(`Geometry` is a `Variant` type that can hold any of the geometric data types: `Point`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, or `Ring`.)DOCS_MD",
        .syntax = "Geometry",
        .examples = {},
        .related = {"Point"},
    });

    factory.registerAlias("GEOMETRY", "Geometry"); /// For MySQL

}

}
