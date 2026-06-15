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

/// Covered-by function: returns true if no point of geometry1 lies in
/// the exterior of geometry2. Equivalent to geoCovers(geometry2, geometry1).
/// Unlike geoWithin, boundary points ARE considered covered.
///
/// Modeled after BigQuery's ST_COVEREDBY.
/// Uses boost::geometry::covered_by(geometry1, geometry2) internally.
/// Templated on Point to support both Spherical and Cartesian coordinate systems.
template <typename Point>
class FunctionGeoCoveredBy : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionGeoCoveredBy() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGeoCoveredBy>(); }

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

                    /// A non-Point geometry cannot be covered by a Point — always false.
                    /// boost::geometry::covered_by(Ring/Polygon/MultiPolygon, Point) is not
                    /// implemented, and it is logically impossible.
                    if constexpr (!left_is_point && right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        executeGeometryPredicate<Point, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b) { return boost::geometry::covered_by(a, b); });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

template <>
const char * FunctionGeoCoveredBy<CartesianPoint>::name = "geoCoveredByCartesian";

template <>
const char * FunctionGeoCoveredBy<SphericalPoint>::name = "geoCoveredBySpherical";

}

REGISTER_FUNCTION(GeoCoveredBy)
{
    factory.registerFunction<FunctionGeoCoveredBy<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry1 lies in the exterior of geometry2 using Cartesian
(flat/planar) coordinates. Unlike `geoWithinCartesian`, boundary points of geometry2
ARE considered covered — a point on the edge of geometry2 returns true.

Equivalent to `geoCoversCartesian(geometry2, geometry1)`.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.

    )",
        .syntax = "geoCoveredByCartesian(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is covered by geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point covered by polygon (interior)",
            R"(
                SELECT geoCoveredByCartesian(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoCoveredByCartesian()─┐
                │                       1 │
                └─────────────────────────┘
            )"},
           {"Boundary point is covered (unlike geoWithinCartesian)",
            R"(
                SELECT geoCoveredByCartesian(
                    CAST((0.0, 0.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoCoveredByCartesian()─┐
                │                       1 │
                └─────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});

    /// ST_CoveredBy dispatches to geoCoveredBySpherical or geoCoveredByCartesian
    /// based on the `st_function_use_spherical` setting.
    factory.registerFunction("ST_CoveredBy", [](ContextPtr context) -> FunctionPtr
    {
        if (context->getSettingsRef()[Setting::st_function_use_spherical])
            return FunctionGeoCoveredBy<SphericalPoint>::create(context);
        else
            return FunctionGeoCoveredBy<CartesianPoint>::create(context);
    }, FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry1 lies in the exterior of geometry2. Unlike `ST_Within`,
boundary points of geometry2 ARE considered covered — a point on the edge returns true.

By default operates in spherical coordinates (longitude, latitude in degrees), consistent
with BigQuery's `ST_COVEREDBY`. Set `st_function_use_spherical = false` to use Cartesian coordinates.

Equivalent to `ST_Covers(geometry2, geometry1)`.

Supports Point, Ring, Polygon, and MultiPolygon types in any combination.
    )",
        .syntax = "ST_CoveredBy(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is covered by geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point covered by polygon",
            R"(
                SELECT ST_CoveredBy(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_CoveredBy()─┐
                │              1 │
                └────────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo},
    FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionGeoCoveredBy<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry1 lies in the exterior of geometry2 on the sphere.
Unlike `geoWithinSpherical`, boundary points of geometry2 ARE considered covered — a point
on the edge of geometry2 returns true.

Equivalent to `geoCoversSpherical(geometry2, geometry1)`.

Similar to BigQuery's `ST_COVEREDBY`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).
    )",
        .syntax = "geoCoveredBySpherical(geometry1, geometry2)",
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
        = {"Returns 1 if geometry1 is covered by geometry2, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point covered by polygon (spherical)",
            R"(
                SELECT geoCoveredBySpherical(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoCoveredBySpherical()─┐
                │                       1 │
                └─────────────────────────┘
            )"},
           {"Boundary point is covered (unlike geoWithinSpherical)",
            R"(
                SELECT geoCoveredBySpherical(
                    CAST((0.0, 0.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─geoCoveredBySpherical()─┐
                │                       1 │
                └─────────────────────────┘
            )"}},
        .introduced_in = {25, 9},
        .category = FunctionDocumentation::Category::Geo});
}

}
