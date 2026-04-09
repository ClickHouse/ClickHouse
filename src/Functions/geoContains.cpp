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

/// Containment function: returns true if geometry1 fully contains geometry2.
/// Modeled after BigQuery's ST_CONTAINS — geometry1 contains geometry2 when no point
/// of geometry2 is outside geometry1, and their interiors intersect.
///
/// Note: boundary points of geometry1 are NOT considered contained (use polygonsWithinSpherical
/// or geoCoversSpherical for boundary-inclusive semantics).
///
/// Uses boost::geometry::within(geometry2, geometry1) internally, which checks that
/// geometry2 is completely inside geometry1's interior.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoContains : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoContains() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoContains>(); }

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

                    /// A Point cannot contain any non-Point geometry — always false.
                    /// boost::geometry::within(Ring/Polygon/MultiPolygon, Point) is not implemented
                    /// and would fail to compile, but it's also logically impossible.
                    if constexpr (left_is_point && !right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        /// geoContains(A, B) = B is within A
                        executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b) { return boost::geometry::within(b, a); });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoContains<CartesianPoint>::name = "geoContainsCartesian";

template <>
const char * FunctionGeoContains<SphericalPoint>::name = "geoContainsSpherical";

}

REGISTER_FUNCTION(GeoContains)
{
    factory.registerFunction<FunctionGeoContains<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if geometry1 fully contains geometry2 using Cartesian (flat/planar) coordinates,
meaning no point of geometry2 lies outside geometry1 and their interiors intersect. Points on
the boundary of geometry1 are NOT considered contained.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.

`ST_Contains` is an alias for this function.
    )",
        .syntax = "geoContainsCartesian(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 contains geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Polygon contains point",
            R"(
                SELECT geoContainsCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((5.0, 5.0), 'Point'))
            )",
            R"(
                ┌─geoContainsCartesian()─┐
                │                      1 │
                └────────────────────────┘
            )"},
           {"Polygon does not contain exterior point",
            R"(
                SELECT geoContainsCartesian(
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
                    CAST((50.0, 50.0), 'Point'))
            )",
            R"(
                ┌─geoContainsCartesian()─┐
                │                      0 │
                └────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_Contains is an alias for geoContainsCartesian.
    factory.registerAlias("ST_Contains", "geoContainsCartesian", FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoContains<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if geometry1 fully contains geometry2 on the sphere, meaning no point
of geometry2 lies outside geometry1 and their interiors intersect. Points on the
boundary of geometry1 are NOT considered contained.

Similar to BigQuery's `ST_CONTAINS`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).
    )",
        .syntax = "geoContainsSpherical(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 contains geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Polygon contains point (spherical)",
            R"(
                SELECT geoContainsSpherical(
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
                    CAST((0.005, 0.005), 'Point'))
            )",
            R"(
                ┌─geoContainsSpherical()─┐
                │                      1 │
                └────────────────────────┘
            )"},
           {"Polygon does not contain exterior point (spherical)",
            R"(
                SELECT geoContainsSpherical(
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
                    CAST((5.0, 5.0), 'Point'))
            )",
            R"(
                ┌─geoContainsSpherical()─┐
                │                      0 │
                └────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
