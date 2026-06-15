#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/geometryConstOptimization.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <memory>

namespace DB
{

namespace Setting
{
    extern const SettingsBool st_function_use_spherical;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Within function: returns true if geometry1 is completely inside geometry2.
/// Modeled after BigQuery's ST_WITHIN — equivalent to stContains(geometry2, geometry1).
///
/// Uses boost::geometry::within(geometry1, geometry2) internally.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoWithin : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoWithin() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoWithin>(); }

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

                    /// A non-Point geometry cannot be within a Point — always false.
                    /// boost::geometry::within(Ring/Polygon/MultiPolygon, Point) is not
                    /// implemented and would fail to compile.
                    if constexpr (!left_is_point && right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b) { return boost::geometry::within(a, b); });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoWithin<CartesianPoint>::name = "geoWithinCartesian";

template <>
const char * FunctionGeoWithin<SphericalPoint>::name = "geoWithinSpherical";

}

REGISTER_FUNCTION(GeoWithin)
{
    factory.registerFunction<FunctionGeoWithin<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if geometry1 is completely inside geometry2 using Cartesian (flat/planar) coordinates,
meaning no point of geometry1 lies outside geometry2 and their interiors intersect. Points on
the boundary of geometry2 are NOT considered within.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.
    )",
        .syntax = "geoWithinCartesian(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is within geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point within polygon",
            R"(
                SELECT geoWithinCartesian(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoWithinCartesian()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Point not within polygon",
            R"(
                SELECT geoWithinCartesian(
                    CAST((15.0, 15.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoWithinCartesian()─┐
                │                    0 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_Within dispatches to geoWithinSpherical or geoWithinCartesian
    /// based on the `st_function_use_spherical` setting.
    factory.registerFunction("ST_Within", [](ContextPtr context) -> FunctionPtr
    {
        if (context->getSettingsRef()[Setting::st_function_use_spherical])
            return FunctionGeoWithin<SphericalPoint>::create(context);
        else
            return FunctionGeoWithin<CartesianPoint>::create(context);
    }, FunctionDocumentation{
        .description = R"(
Returns true if geometry1 is completely inside geometry2, meaning no point of geometry1 lies
outside geometry2 and their interiors intersect. Points on the boundary of geometry2 are
NOT considered within.

By default operates in spherical coordinates (longitude, latitude in degrees), consistent
with BigQuery's `ST_WITHIN`. Set `st_function_use_spherical = false` to use Cartesian coordinates.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.
    )",
        .syntax = "ST_Within(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is within geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point within polygon",
            R"(
                SELECT ST_Within(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_Within()─┐
                │           1 │
                └─────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo},
    FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoWithin<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if geometry1 is completely inside geometry2 on the sphere, meaning no
point of geometry1 lies outside geometry2 and their interiors intersect. Points on
the boundary of geometry2 are NOT considered within.

Similar to BigQuery's `ST_WITHIN`. Equivalent to `geoContainsSpherical(geometry2, geometry1)`.
Supports Point, Ring, Polygon, and MultiPolygon types in any combination.
Operates in spherical coordinates (longitude, latitude in degrees).
    )",
        .syntax = "geoWithinSpherical(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is within geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point within polygon (spherical)",
            R"(
                SELECT geoWithinSpherical(
                    CAST((0.005, 0.005), 'Point'),
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoWithinSpherical()─┐
                │                    1 │
                └──────────────────────┘
            )"},
           {"Point not within polygon (spherical)",
            R"(
                SELECT geoWithinSpherical(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoWithinSpherical()─┐
                │                    0 │
                └──────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
