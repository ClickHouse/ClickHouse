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

/// Covers function: returns true if no point of geometry2 lies in the exterior of
/// geometry1. Unlike geoContains, boundary points ARE considered covered.
///
/// Modeled after BigQuery's ST_COVERS.
/// Uses boost::geometry::covered_by(geometry2, geometry1) internally.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoCovers : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoCovers() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoCovers>(); }

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

                    /// A Point cannot cover any non-Point geometry — always false.
                    /// boost::geometry::covered_by(Ring/Polygon/MultiPolygon, Point) is not
                    /// implemented, and it is logically impossible.
                    if constexpr (left_is_point && !right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        /// geoCovers(A, B) = B is covered_by A
                        executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b) { return boost::geometry::covered_by(b, a); });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoCovers<CartesianPoint>::name = "geoCoversCartesian";

template <>
const char * FunctionGeoCovers<SphericalPoint>::name = "geoCoversSpherical";

}

REGISTER_FUNCTION(GeoCovers)
{
    factory.registerFunction<FunctionGeoCovers<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry2 lies in the exterior of geometry1 using Cartesian
(flat/planar) coordinates. Unlike `geoContainsCartesian`, boundary points of geometry1
ARE considered covered — a point on the edge of a polygon returns true.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.

`ST_Covers` is an alias for this function.
    )",
        .syntax = "geoCoversCartesian(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 covers geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Polygon covers interior point",
            R"(
                SELECT geoCoversCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((5.0, 5.0), 'Point'))
            )",
            R"(
                ┌─geoCoversCartesian()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Polygon covers boundary point (unlike geoContainsCartesian)",
            R"(
                SELECT geoCoversCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((0.0, 0.0), 'Point'))
            )",
            R"(
                ┌─geoCoversCartesian()─┐
                │                    1 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_Covers is an alias for geoCoversCartesian.
    factory.registerAlias("ST_Covers", "geoCoversCartesian", FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoCovers<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry2 lies in the exterior of geometry1 on the sphere.
Unlike `geoContainsSpherical`, boundary points of geometry1 ARE considered covered — a
point on the edge of a polygon returns true.

Similar to BigQuery's `ST_COVERS`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).
    )",
        .syntax = "geoCoversSpherical(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 covers geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Polygon covers interior point (spherical)",
            R"(
                SELECT geoCoversSpherical(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((5.0, 5.0), 'Point'))
            )",
            R"(
                ┌─geoCoversSpherical()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Polygon covers boundary point (spherical, unlike geoContainsSpherical)",
            R"(
                SELECT geoCoversSpherical(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((0.0, 0.0), 'Point'))
            )",
            R"(
                ┌─geoCoversSpherical()─┐
                │                    1 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
