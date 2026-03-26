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

/// Spherical intersects function that supports Point, Ring, Polygon, and MultiPolygon
/// in any combination. Modeled after BigQuery's ST_INTERSECTS but operates on
/// ClickHouse's native geometry types in spherical coordinates (lon/lat degrees).
///
/// Unlike polygonsIntersectSpherical, this function accepts Point as an argument,
/// enabling point-in-polygon and point-point intersection tests.
class FunctionSTIntersects : public IFunction
{
public:
    static inline const char * name = "ST_Intersects";

    explicit FunctionSTIntersects() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionSTIntersects>(); }

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

                /// Reject LineString and MultiLineString — boost::geometry spherical
                /// intersects support for these types is limited and may produce
                /// incorrect results or compilation errors.
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
                    auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                    auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        /// boost::geometry::correct fixes ring orientation and closure for
                        /// polygon-like types. It must NOT be called on Point — it would
                        /// fail to compile since Point has no rings to correct.
                        if constexpr (!std::is_same_v<ColumnToPointsConverter<SphericalPoint>, LeftConverter>)
                            boost::geometry::correct(first[i]);
                        if constexpr (!std::is_same_v<ColumnToPointsConverter<SphericalPoint>, RightConverter>)
                            boost::geometry::correct(second[i]);

                        res_data.emplace_back(boost::geometry::intersects(first[i], second[i]));
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}

REGISTER_FUNCTION(STIntersects)
{
    factory.registerFunction<FunctionSTIntersects>(FunctionDocumentation{
        .description = R"(
Returns true if two geometries intersect on the sphere, i.e., they share at least
one point in common. Supports Point, Ring, Polygon, and MultiPolygon types in any
combination. Operates in spherical coordinates (longitude, latitude in degrees).

Similar to BigQuery's `ST_INTERSECTS`. Unlike `polygonsIntersectSpherical`, this
function also accepts `Point` arguments for point-in-polygon tests.

Note: function name is case-insensitive, so `st_intersects`, `ST_INTERSECTS`, etc. all work.
    )",
        .syntax = "ST_Intersects(geometry1, geometry2)",
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
                SELECT stIntersects(
                    CAST((0.005, 0.005), 'Point'),
                    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
            )",
            R"(
                ┌─stIntersects()─┐
                │              1 │
                └────────────────┘
            )"},
           {"Polygon-polygon intersection",
            R"(
                SELECT stIntersects(
                    CAST([[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]], 'Polygon'),
                    CAST([[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]], 'Polygon'))
            )",
            R"(
                ┌─stIntersects()─┐
                │              1 │
                └────────────────┘
            )"}},
        .introduced_in = {25, 8},
        .category = FunctionDocumentation::Category::Geo});
}

}
