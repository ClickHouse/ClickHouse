#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupPolygonIntersection.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/geometryConverters.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct Settings;

void registerAggregateFunctionGroupPolygonIntersection(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Computes the geometric intersection of all polygons in a group, producing a single `MultiPolygon` that covers the area shared by all input geometries.

If no rows are aggregated, an empty `MultiPolygon` is returned. If any row's geometry does not overlap with the accumulated intersection, the result becomes an empty `MultiPolygon`.

Input geometries of type `Ring` or `Polygon` are internally upcast to `MultiPolygon` before the intersection is computed.

The function uses [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/) to compute the geometric intersection.
    )";
    FunctionDocumentation::Syntax syntax = "groupPolygonIntersection(geometry [, correct_geometry])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "A column of type Ring, Polygon, or MultiPolygon.", {"Ring", "Polygon", "MultiPolygon"}},
        {"correct_geometry", "Optional. A UInt8 value that controls whether `boost::geometry::correct` is applied to input geometries (e.g. ensuring correct ring orientation and closure). `1` (default) enables correction, `0` disables it.", {"UInt8"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"A MultiPolygon representing the intersection of all input geometries.", {"MultiPolygon"}};
    FunctionDocumentation::Examples examples = {
    {
        "Intersection of two overlapping squares",
        R"(
CREATE TABLE test_polygons (geom Polygon) ENGINE = Memory;

INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

SELECT wkt(groupPolygonIntersection(geom)) AS result FROM test_polygons;
        )",
        R"(
┌─result─────────────────────────────────────┐
│ MULTIPOLYGON(((5 10,10 10,10 5,5 5,5 10))) │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("groupPolygonIntersection",
        {[](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);

            if (argument_types.size() != 1 && argument_types.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Aggregate function {} requires 1 or 2 arguments, got {}", name, argument_types.size());

            bool correct_geometry = true;
            if (argument_types.size() == 2)
            {
                if (!isUInt8(argument_types[1]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Second argument (correct_geometry) for aggregate function {} must be UInt8", name);
            }

            return std::make_shared<AggregateFunctionGroupPolygonIntersection<CartesianPoint>>(argument_types, correct_geometry);
        }, {}, documentation});
}

}
