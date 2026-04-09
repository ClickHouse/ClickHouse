#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometryConstOptimization.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Intersects function that supports Point, Ring, Polygon, and MultiPolygon
/// in any combination. Modeled after BigQuery's ST_INTERSECTS.
///
/// Unlike polygonsIntersectSpherical, this function accepts Point as an argument,
/// enabling point-in-polygon and point-point intersection tests.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoIntersects : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoIntersects() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoIntersects>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override { return std::make_shared<DataTypeUInt8>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnUInt8::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        bool left_is_const = isColumnConst(*arguments[0].column);
        bool right_is_const = isColumnConst(*arguments[1].column);

        callOnTwoGeometryDataTypes<Point>(
            arguments[0].type,
            arguments[1].type,
            [&](const auto & left_type, const auto & right_type)
            {
                using LeftConverterType = std::decay_t<decltype(left_type)>;
                using RightConverterType = std::decay_t<decltype(right_type)>;

                using LeftConverter = typename LeftConverterType::Type;
                using RightConverter = typename RightConverterType::Type;

                /// Reject LineString and MultiLineString — boost::geometry
                /// intersects support for these types is limited and may produce
                /// incorrect results or compilation errors.
                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<Point>, LeftConverter>
                    || std::is_same_v<ColumnToLineStringsConverter<Point>, RightConverter>)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
                else if constexpr (
                    std::is_same_v<ColumnToMultiLineStringsConverter<Point>, LeftConverter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, RightConverter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
                else
                {
                    constexpr bool left_is_point = std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter>;
                    constexpr bool right_is_point = std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>;

                    executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                        arguments, res_data, input_rows_count, left_is_const, right_is_const,
                        [](const auto & a, const auto & b) { return boost::geometry::intersects(a, b); });
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoIntersects<CartesianPoint>::name = "geoIntersectsCartesian";

template <>
const char * FunctionGeoIntersects<SphericalPoint>::name = "geoIntersectsSpherical";

}

REGISTER_FUNCTION(GeoIntersects)
{
    factory.registerFunction<FunctionGeoIntersects<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries intersect using Cartesian (flat/planar) coordinates,
i.e., they share at least one point in common. Supports Point, Ring, Polygon, and
MultiPolygon types in any combination.

Unlike `polygonsIntersectCartesian`, this function also accepts `Point` arguments
for point-in-polygon tests.

`ST_Intersects` is an alias for this function.
    )",
        .syntax = "geoIntersectsCartesian(geometry1, geometry2)",
        .arguments
        = {{"geometry1",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"geometry2",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value
        = {"Returns 1 if the two geometries intersect, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point in polygon",
            R"(
                SELECT geoIntersectsCartesian(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoIntersectsCartesian()─┐
                │                        1 │
                └──────────────────────────┘
            )"},
           {"Polygon-polygon intersection",
            R"(
                SELECT geoIntersectsCartesian(
                    CAST([[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]], 'Polygon'),
                    CAST([[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]], 'Polygon'))
            )",
            R"(
                ┌─geoIntersectsCartesian()─┐
                │                        1 │
                └──────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_Intersects is an alias for geoIntersectsCartesian.
    factory.registerAlias("ST_Intersects", "geoIntersectsCartesian", FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoIntersects<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries intersect on the sphere, i.e., they share at least
one point in common. Supports Point, Ring, Polygon, and MultiPolygon types in any
combination. Operates in spherical coordinates (longitude, latitude in degrees).

Similar to BigQuery's `ST_INTERSECTS`. Unlike `polygonsIntersectSpherical`, this
function also accepts `Point` arguments for point-in-polygon tests.
    )",
        .syntax = "geoIntersectsSpherical(geometry1, geometry2)",
        .arguments
        = {{"geometry1",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"geometry2",
            "A value of type [`Point`](/sql-reference/data-types/geo#point), "
            "[`Ring`](/sql-reference/data-types/geo#ring), "
            "[`Polygon`](/sql-reference/data-types/geo#polygon), or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value
        = {"Returns 1 if the two geometries intersect, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point in polygon (spherical)",
            R"(
                SELECT geoIntersectsSpherical(
                    CAST((0.005, 0.005), 'Point'),
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoIntersectsSpherical()─┐
                │                        1 │
                └──────────────────────────┘
            )"},
           {"Polygon-polygon intersection (spherical)",
            R"(
                SELECT geoIntersectsSpherical(
                    CAST([[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]], 'Polygon'),
                    CAST([[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]], 'Polygon'))
            )",
            R"(
                ┌─geoIntersectsSpherical()─┐
                │                        1 │
                └──────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
