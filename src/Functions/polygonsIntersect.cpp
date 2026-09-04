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

template <typename Point>
class FunctionpolygonsIntersect : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionpolygonsIntersect() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionpolygonsIntersect>(); }

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
                    std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter>
                    || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be Point", getName());
                else if constexpr (
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
                    auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                    auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                    for (size_t i = 0; i < input_rows_count; ++i)
                    {
                        boost::geometry::correct(first[i]);
                        boost::geometry::correct(second[i]);

                        res_data.emplace_back(boost::geometry::intersects(first[i], second[i]));
                    }
                }
            });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};


template <>
const char * FunctionpolygonsIntersect<CartesianPoint>::name = "polygonsIntersectCartesian";

template <>
const char * FunctionpolygonsIntersect<SphericalPoint>::name = "polygonsIntersectSpherical";

}

REGISTER_FUNCTION(polygonsIntersect)
{
    factory.registerFunction<FunctionpolygonsIntersect<CartesianPoint>>(FunctionDocumentation{
        .description = R"(
        Returns true if the two [`Polygon`](sql-reference/data-types/geo#polygon) or [`MultiPolygon`](sql-reference/data-types/geo#multipolygon) intersect (share any common area or boundary).
    )",
        .syntax = "polygonsIntersectCartesian(polygon1, polygon2)",
        .arguments
        = {{"polygon1",
            "A value of type [`Polygon`](/sql-reference/data-types/geo#polygon) or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"polygon2",
            "A value of type [`Polygon`](/sql-reference/data-types/geo#polygon) or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value = {"Returns true (1) if the two polygons intersect. [`Bool`](/sql-reference/data-types/boolean)."},
        .examples
        = {{"Usage example",
            R"(
                SELECT polygonsIntersectCartesian([[[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]])

        )",
            R"(
                ┌─polygonsIntersectCartesian()─┐
                │ 1 │
                └───────────────────┘
        )"}},
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Geo});

    factory.registerFunction<FunctionpolygonsIntersect<SphericalPoint>>(FunctionDocumentation{
        .description = R"(
        Returns true if the two [`Polygon`](sql-reference/data-types/geo#polygon) or [`MultiPolygon`](sql-reference/data-types/geo#multipolygon) intersect (share any common area or boundary).
    )",
        .syntax = "polygonsIntersectSpherical(polygon1, polygon2)",
        .arguments
        = {{"polygon1",
            "A value of type [`Polygon`](/sql-reference/data-types/geo#polygon) or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."},
           {"polygon2",
            "A value of type [`Polygon`](/sql-reference/data-types/geo#polygon) or "
            "[`MultiPolygon`](/sql-reference/data-types/geo#multipolygon)."}},
        .returned_value = {"Returns true (1) if the two polygons intersect (share any common area or boundary). "
                           "[`Bool`](/sql-reference/data-types/boolean)."},
        .examples
        = {{"Usage example",
            R"(
                SELECT polygonsIntersectSpherical([[[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]])

        )",
            R"(
                ┌─polygonsIntersectSpherical()─┐
                │ 1 │
                └───────────────────┘
        )"}},
        .introduced_in = {25, 6},
        .category = FunctionDocumentation::Category::Geo});
}

}
