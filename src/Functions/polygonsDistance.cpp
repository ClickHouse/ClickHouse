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
class FunctionPolygonsDistance : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsDistance() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionPolygonsDistance>();
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
        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
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
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    res_data.emplace_back(boost::geometry::distance(first[i], second[i]));
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
const char * FunctionPolygonsDistance<CartesianPoint>::name = "polygonsDistanceCartesian";

template <>
const char * FunctionPolygonsDistance<SphericalPoint>::name = "polygonsDistanceSpherical";

}

REGISTER_FUNCTION(PolygonsDistance)
{
    FunctionDocumentation::Description description_cartesian = R"(
Calculates distance between two polygons.
    )";
    FunctionDocumentation::Syntax syntax_cartesian = "polygonsDistanceCartesian(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_cartesian = {
        {"polygon1", "A Polygon value", {"Polygon"}},
        {"polygon2", "A Polygon value", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cartesian = {"Returns the minimal distance between the two polygons", {"Float64"}};
    FunctionDocumentation::Examples examples_cartesian =
    {
    {
        "Distance example",
        R"(
SELECT polygonsDistanceCartesian([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(10., 10.), (10., 40.), (40., 40.), (40., 10.), (10., 10.)]]])
        )",
        R"(
14.000714267493642
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cartesian = {21, 4};
    FunctionDocumentation::Category category_cartesian = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_cartesian = {description_cartesian, syntax_cartesian, arguments_cartesian, {}, returned_value_cartesian, examples_cartesian, introduced_in_cartesian, category_cartesian};

    factory.registerFunction<FunctionPolygonsDistance<CartesianPoint>>(function_documentation_cartesian);

    FunctionDocumentation::Description description_spherical = R"(
Calculates the minimal distance between two points where one point belongs to the first polygon and the second to another polygon.
Spherical means that coordinates are interpreted as coordinates on a pure and ideal sphere, which is not true for the Earth.
Using this type of coordinate system speeds up execution, but of course is not precise.
    )";
    FunctionDocumentation::Syntax syntax_spherical = "polygonsDistanceSpherical(polygon1, polygon2)";
    FunctionDocumentation::Arguments arguments_spherical = {
        {"polygon1", "The first Polygon value.", {"Polygon"}},
        {"polygon2", "The second Polygon value.", {"Polygon"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_spherical = {"Returns the minimal distance between the two polygons on a sphere", {"Float64"}};
    FunctionDocumentation::Examples examples_spherical =
    {
    {
        "Spherical distance",
        R"(
SELECT polygonsDistanceSpherical([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(10., 10.), (10., 40.), (40., 40.), (40., 10.), (10., 10.)]]])
        )",
        R"(
0.24372872211133834
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_spherical = {21, 4};
    FunctionDocumentation::Category category_spherical = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation function_documentation_spherical = {description_spherical, syntax_spherical, arguments_spherical, {}, returned_value_spherical, examples_spherical, introduced_in_spherical, category_spherical};

    factory.registerFunction<FunctionPolygonsDistance<SphericalPoint>>(function_documentation_spherical);
}

}
