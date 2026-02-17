#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupConvexHull.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

void registerAggregateFunctionGroupConvexHull(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the [convex hull](https://en.wikipedia.org/wiki/Convex_hull) of all geometries in a group, producing a `Ring` (closed polygon without holes) that is the smallest convex polygon containing all input geometry points.

All input geometry types are internally converted to polygons before the hull is computed. Specifically:
- `Point` values become degenerate single-vertex polygons.
- `Ring` and `LineString` values become the outer ring of a polygon.
- `Polygon` values contribute only their outer ring (holes are ignored, as they do not affect the convex hull).
- `MultiPolygon` and `MultiLineString` values are decomposed into individual polygons.

If no rows are aggregated, an empty `Ring` is returned.

The function uses [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/) to compute the convex hull.

See also:
- [Geo Types](/sql-reference/data-types/geo)
    )";
    FunctionDocumentation::Syntax syntax = "groupConvexHull(geometry [, correct_geometry])";
    FunctionDocumentation::Arguments arguments
        = {{"geometry",
            "Column to compute the convex hull of.",
            {"Point", "Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString"}},
           {"correct_geometry",
            "Optional. A [UInt8](/sql-reference/data-types/int-uint) value that controls whether `boost::geometry::correct` is applied to "
            "input geometries (e.g. ensuring correct ring orientation and closure). `1` (default) enables correction, `0` disables it.",
            {"UInt8"}}};
    FunctionDocumentation::Parameters doc_parameters = {};
    FunctionDocumentation::ReturnedValue returned_value
        = {"A Ring representing the outer boundary of the convex hull of all input geometries.", {"Ring"}};
    FunctionDocumentation::Examples examples
        = {{"Convex hull of points",
            R"(
CREATE TABLE geo_points (p Point) ENGINE = Memory;

INSERT INTO geo_points VALUES ((0, 0)), ((10, 0)), ((10, 10)), ((0, 10)), ((5, 5));

SELECT wkt(groupConvexHull(p)) AS hull FROM geo_points;
        )",
            R"(
┌─hull───────────────────────────────┐
│ POLYGON((0 0,0 10,10 10,10 0,0 0)) │
└────────────────────────────────────┘
        )"},
           {"Convex hulls per group",
            R"(
CREATE TABLE geo_grouped (grp Int32, p Point) ENGINE = Memory;

INSERT INTO geo_grouped VALUES
    (1, (0, 0)), (1, (10, 0)), (1, (10, 10)), (1, (0, 10)),
    (2, (100, 100)), (2, (110, 100)), (2, (110, 110)), (2, (100, 110));

SELECT grp, wkt(groupConvexHull(p)) AS hull FROM geo_grouped GROUP BY grp ORDER BY grp;
        )",
            R"(
┌─grp─┬─hull───────────────────────────────────────────────┐
│   1 │ POLYGON((0 0,0 10,10 10,10 0,0 0))                 │
│   2 │ POLYGON((100 100,100 110,110 110,110 100,100 100)) │
└─────┴────────────────────────────────────────────────────┘
        )"},
           {"Convex hull from polygon inputs (holes are ignored)",
            R"(
CREATE TABLE geo_polygons (polygon Polygon) ENGINE = Memory;

INSERT INTO geo_polygons VALUES
    ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)], [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]]);

SELECT wkt(groupConvexHull(polygon)) AS hull FROM geo_polygons;
        )",
            R"(
┌─hull───────────────────────────────┐
│ POLYGON((0 0,0 10,10 10,10 0,0 0)) │
└────────────────────────────────────┘
        )"},
           {"Alias",
            "SELECT wkt(ST_ConvexHull(polygon)) AS hull FROM geo_polygons;",
            R"(
┌─hull───────────────────────────────┐
│ POLYGON((0 0,0 10,10 10,10 0,0 0)) │
└────────────────────────────────────┘
               )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation
        = {description, syntax, arguments, doc_parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(
        "groupConvexHull",
        {[](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
         {
             assertNoParameters(name, parameters);

             if (argument_types.size() != 1 && argument_types.size() != 2)
                 throw Exception(
                     ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                     "Aggregate function {} requires 1 or 2 arguments, got {}",
                     name,
                     argument_types.size());

             bool correct_geometry = true;
             if (argument_types.size() == 2)
             {
                 if (!isUInt8(argument_types[1]))
                     throw Exception(
                         ErrorCodes::BAD_ARGUMENTS, "Second argument (correct_geometry) for aggregate function {} must be UInt8", name);
             }

             return std::make_shared<AggregateFunctionGroupConvexHull<CartesianPoint>>(argument_types, correct_geometry);
         },
         {},
         documentation});

    factory.registerAlias("ST_ConvexHull", "groupConvexHull", AggregateFunctionFactory::Case::Insensitive);
}

}
