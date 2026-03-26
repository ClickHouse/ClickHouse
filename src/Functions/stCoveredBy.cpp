#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

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

/// Spherical covered-by function: returns true if no point of geometry1 lies in
/// the exterior of geometry2. Equivalent to ST_Covers(geometry2, geometry1).
/// Unlike ST_Within, boundary points ARE considered covered.
///
/// Modeled after BigQuery's ST_COVEREDBY.
/// Uses boost::geometry::covered_by(geometry1, geometry2) internally.
class FunctionSTCoveredBy : public IFunction
{
public:
    static inline const char * name = "ST_CoveredBy";

    explicit FunctionSTCoveredBy() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSTCoveredBy>(); }

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

                    /// A non-Point geometry cannot be covered by a Point — always false.
                    /// boost::geometry::covered_by(Ring/Polygon/MultiPolygon, Point) is not
                    /// implemented, and it is logically impossible.
                    if constexpr (!left_is_point && right_is_point)
                    {
                        res_data.resize_fill(input_rows_count, 0);
                    }
                    else
                    {
                        auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                        auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                        for (size_t i = 0; i < input_rows_count; ++i)
                        {
                            if constexpr (!left_is_point)
                                boost::geometry::correct(first[i]);
                            if constexpr (!right_is_point)
                                boost::geometry::correct(second[i]);

                            res_data.emplace_back(boost::geometry::covered_by(first[i], second[i]));
                        }
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}

REGISTER_FUNCTION(STCoveredBy)
{
    factory.registerFunction<FunctionSTCoveredBy>(FunctionDocumentation{
        .description = R"(
Returns true if no point of geometry1 lies in the exterior of geometry2 on the sphere.
Unlike `ST_Within`, boundary points of geometry2 ARE considered — a point on the edge
of geometry2 returns true.

Equivalent to `ST_Covers(geometry2, geometry1)`.

Similar to BigQuery's `ST_COVEREDBY`. Supports Point, Ring, Polygon, and MultiPolygon
types in any combination. Operates in spherical coordinates (longitude, latitude in degrees).

Note: function name is case-insensitive, so `st_coveredby`, `ST_COVEREDBY`, etc. all work.
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
        = {{"Point covered by polygon (interior)",
            R"(
                SELECT ST_CoveredBy(
                    CAST((5.0, 5.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_CoveredBy()─┐
                │              1 │
                └────────────────┘
            )"},
           {"Boundary point is covered (unlike ST_Within)",
            R"(
                SELECT ST_CoveredBy(
                    CAST((0.0, 0.0), 'Point'),
                    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─ST_CoveredBy()─┐
                │              1 │
                └────────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo});
}

}
