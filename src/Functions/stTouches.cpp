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

/// Spherical touches function: returns true if two geometries intersect only at
/// their boundaries, with no common interior points.
///
/// Modeled after BigQuery's ST_TOUCHES.
/// Uses boost::geometry::touches(geometry1, geometry2) internally.
class FunctionSTTouches : public IFunction
{
public:
    static inline const char * name = "ST_Touches";

    explicit FunctionSTTouches() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSTTouches>(); }

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

        callOnTwoGeometryDataTypes<SphericalPoint>(
            arguments[0].type,
            arguments[1].type,
            [&](const auto & left_type, const auto & right_type)
            {
                using LeftConverterType = std::decay_t<decltype(left_type)>;
                using RightConverterType = std::decay_t<decltype(right_type)>;

                using LeftConverter = typename LeftConverterType::Type;
                using RightConverter = typename RightConverterType::Type;

                if constexpr (
                    std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, LeftConverter>
                    || std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, RightConverter>)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
                else if constexpr (
                    std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, LeftConverter>
                    || std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, RightConverter>)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
                else
                {
                    constexpr bool left_is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, LeftConverter>;
                    constexpr bool right_is_point = std::is_same_v<ColumnToPointsConverter<SphericalPoint>, RightConverter>;

                    /// Two points never "touch" — they either coincide (share interior)
                    /// or are disjoint. By DE-9IM definition, touches requires at least
                    /// one geometry to have an interior, which points lack.
                    if constexpr (left_is_point && right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        executeGeometryPredicate<SphericalPoint, LeftConverter, RightConverter, left_is_point, right_is_point>(
                            arguments, res_data, input_rows_count, left_is_const, right_is_const,
                            [](const auto & a, const auto & b) { return boost::geometry::touches(a, b); });
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return false; }
};

}

REGISTER_FUNCTION(STTouches)
{
    factory.registerFunction<FunctionSTTouches>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries intersect only at their boundaries on the sphere,
with no common interior points. For example, two polygons that share an edge but
do not overlap return true.

A point on the boundary of a polygon is considered touching. Two identical points
do NOT touch (they share their entire extent, not just boundaries).

Similar to BigQuery's `ST_TOUCHES`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).

Note: function name is case-insensitive, so `st_touches`, `ST_TOUCHES`, etc. all work.
    )",
        .syntax = "ST_Touches(geometry1, geometry2)",
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
        = {"Returns 1 if the geometries touch, 0 otherwise. [`UInt8`](/sql-reference/data-types/int-uint)."},
        .examples
        = {{"Point on polygon boundary",
            R"(
                SELECT ST_Touches(
                    CAST((0.0, 0.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_Touches()─┐
                │            1 │
                └──────────────┘
            )"},
           {"Point inside polygon (not touching)",
            R"(
                SELECT ST_Touches(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_Touches()─┐
                │            0 │
                └──────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo});
}

}
