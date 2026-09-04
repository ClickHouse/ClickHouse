#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Common/logger_useful.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
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
class FunctionPolygonsWithin : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsWithin() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonsWithin>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnUInt8::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        callOnTwoGeometryDataTypes<Point>(arguments[0].type, arguments[1].type, [&](const auto & left_type, const auto & right_type)
        {
            using LeftConverterType = std::decay_t<decltype(left_type)>;
            using RightConverterType = std::decay_t<decltype(right_type)>;

            using LeftConverter = typename LeftConverterType::Type;
            using RightConverter = typename RightConverterType::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be Point", getName());
            else if constexpr (std::is_same_v<ColumnToLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
            else if constexpr (std::is_same_v<ColumnToMultiLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    res_data.emplace_back(boost::geometry::within(first[i], second[i]));
                }
            }
        });

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


template <>
const char * FunctionPolygonsWithin<CartesianPoint>::name = "polygonsWithinCartesian";

template <>
const char * FunctionPolygonsWithin<SphericalPoint>::name = "polygonsWithinSpherical";

}

REGISTER_FUNCTION(PolygonsWithin)
{
    FunctionDocumentation::Description description_cartesian = R"(
Checks if a polygon is within another polygon.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonsWithinCartesian(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_cartesian = {
        {"polygon1", "The first polygon.", {"Polygon"}},
        {"polygon2", "The second polygon.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns `1` if `polygon1` is contained in `polygon2`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Usage example",
         R"(
SELECT polygonsWithinCartesian([[[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]])
        )",
        R"(
1
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonsWithin<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Returns true or false depending on whether or not one polygon lies completely inside another polygon.
[Reference](https://www.boost.org/doc/libs/1_62_0/libs/geometry/doc/html/geometry/reference/algorithms/within/within_2.html)
    )";
    FunctionDocumentation::Syntax syntax_spherical = "polygonsWithinSpherical(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_spherical = {
        {"polygon1", "The first Polygon.", {"Polygon"}},
        {"polygon2", "The second Polygon.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"Returns `1` if `polygon1` lies completely within `polygon2`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples_spherical =
    {
    {
        "Usage example",
        R"(
SELECT polygonsWithinSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]], [[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]);
        )",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonsWithin<SphericalPoint>>(function_documentation_spherical);
}

}
